/**
 * Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
 *
 * The use and distribution terms for this software are covered by the
 * Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
 * which can be found in the file epl-v10.txt at the root of this distribution.
 * By using this software in any fashion, you are agreeing to be bound by
 * the terms of this license.
 *
 * You must not remove this notice, or any other, from this software.
 */

package dendrite.java;

import clojure.lang.Agent;
import clojure.lang.Symbol;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

public final class RecordGroup {

  public static final int
    NONE = 0,
    ONLY_DEFAULT = 1,
    ALL = 2;

  public static final class Writer implements IOutputBuffer, IFileWriteable {

    private static final int PARALLEL_THRESHOLD = 128;

    private final IColumnChunkWriter[] columnChunkWriters;
    private final ArrayList<OptimizingColumnChunkWriter> optimizingColumnChunkwriters;
    private long numRecords;

    public Writer(Types types, Schema.Column[] columns, int targetDataPageLength,
                  int optimizationStrategy) {
      columnChunkWriters = new IColumnChunkWriter[columns.length];
      this.optimizingColumnChunkwriters = new ArrayList<OptimizingColumnChunkWriter>();
      for (int i=0; i<columns.length; ++i) {
        if (optimizationStrategy == ALL || (optimizationStrategy == ONLY_DEFAULT
                                            && columns[i].encoding == Types.PLAIN
                                            && columns[i].compression == Types.NONE)) {
          OptimizingColumnChunkWriter optimizingWriter
            = OptimizingColumnChunkWriter.create(types, columns[i], targetDataPageLength);
          columnChunkWriters[i] = optimizingWriter;
          optimizingColumnChunkwriters.add(optimizingWriter);
        } else {
          columnChunkWriters[i] = ColumnChunks.createWriter(types, columns[i], targetDataPageLength);
        }
      }
      numRecords = 0;
    }

    @SuppressWarnings("unchecked")
    private void writeParallel(Bundle bundle) {
      List<Future<Object>> futures = new ArrayList<Future<Object>>(columnChunkWriters.length);
      for (int i=0; i<columnChunkWriters.length; ++i) {
        final IColumnChunkWriter writer = columnChunkWriters[i];
        final List columnValues = bundle.columnValues[i];
        futures.add(Agent.soloExecutor.submit(new Callable<Object>() {
              public Object call() {
                writer.write(columnValues);
                return null;
              }
          }));
      }
      for (Future<Object> fut : futures) {
        Utils.tryGetFuture(fut);
      }
    }

    @SuppressWarnings("unchecked")
    private void writeSequential(Bundle bundle) {
      for (int i=0; i<columnChunkWriters.length; ++i) {
        columnChunkWriters[i].write(bundle.columnValues[i]);
      }
    }

    public void write(Bundle bundle) {
      int numRecordsInBundle = bundle.getNumRecords();
      numRecords += numRecordsInBundle;
      if (numRecordsInBundle >= PARALLEL_THRESHOLD) {
        writeParallel(bundle);
      } else {
        writeSequential(bundle);
      }
    }

    public boolean canOptimize() {
      return optimizingColumnChunkwriters.size() > 0;
    }

    public void optimize(final Map<Symbol,Double> compressionThresholds) {
      if (canOptimize()) {
        List<Future<IColumnChunkWriter>> futures
          = new ArrayList<Future<IColumnChunkWriter>>(columnChunkWriters.length);
        for (final OptimizingColumnChunkWriter occw : optimizingColumnChunkwriters) {
          futures.add(Agent.soloExecutor.submit(new Callable<IColumnChunkWriter>() {
                public IColumnChunkWriter call() {
                  return occw.optimize(compressionThresholds);
                }
            }));
        }
        for (Future<IColumnChunkWriter> fut : futures) {
          IColumnChunkWriter ccw = Utils.tryGetFuture(fut);
          columnChunkWriters[ccw.getColumn().columnIndex] = ccw;
        }
        optimizingColumnChunkwriters.clear();
      }
    }

    public Schema.Column[] columns() {
      Schema.Column[] columns = new Schema.Column[columnChunkWriters.length];
      for (int i=0; i<columnChunkWriters.length; ++i) {
        columns[i] = columnChunkWriters[i].getColumn();
      }
      return columns;
    }

    public long getNumRecords() {
      return numRecords;
    }

    public int getNumColumns() {
      return columnChunkWriters.length;
    }

    public Metadata.RecordGroup getMetadata() {
      Metadata.ColumnChunk[] columnChunksMetadata = new Metadata.ColumnChunk[columnChunkWriters.length];
      for (int i=0; i<columnChunkWriters.length; ++i) {
        columnChunksMetadata[i] = columnChunkWriters[i].getMetadata();
      }
      return new Metadata.RecordGroup(getLength(), numRecords, columnChunksMetadata);
    }

    @Override
    public void reset() {
      numRecords = 0;
      for (IColumnChunkWriter columnChunkWriter : columnChunkWriters) {
        columnChunkWriter.reset();
      }
    }

    @Override
    public void finish() {
      List<Future<Object>> futures = new ArrayList<Future<Object>>(columnChunkWriters.length);
      for (final IColumnChunkWriter ccw : columnChunkWriters) {
        futures.add(Agent.soloExecutor.submit(new Callable<Object>() {
              public Object call() {
                ccw.finish();
                return null;
              }
            }));
      }
      for (Future<Object> fut : futures) {
        Utils.tryGetFuture(fut);
      }
    }

    @Override
    public int getLength() {
      int length = 0;
      for (IColumnChunkWriter columnChunkWriter : columnChunkWriters) {
        length += columnChunkWriter.getLength();
      }
      return length;
    }

    @Override
    public int getEstimatedLength() {
      int estimatedLength = 0;
      for (IColumnChunkWriter columnChunkWriter : columnChunkWriters) {
        estimatedLength += columnChunkWriter.getEstimatedLength();
      }
      return estimatedLength;
    }

    @Override
    public void writeTo(MemoryOutputStream mos) {
      finish();
      for (IColumnChunkWriter columnChunkWriter : columnChunkWriters) {
        mos.write(columnChunkWriter);
      }
    }

    @Override
    public void writeTo(FileChannel fileChannel) throws IOException {
      finish();
      for (IColumnChunkWriter columnChunkWriter : columnChunkWriters) {
        fileChannel.write(columnChunkWriter.toByteBuffer());
      }
    }

  }

  private static int[] getColumnChunkByteOffsets(Metadata.RecordGroup recordGroupMetadata) {
    Metadata.ColumnChunk[] columnChunksMetadata = recordGroupMetadata.columnChunks;
    int[] offsets = new int[columnChunksMetadata.length];
    if (columnChunksMetadata.length > 0) {
      offsets[0] = 0;
      for (int i=1; i<columnChunksMetadata.length; ++i) {
        offsets[i] = offsets[i-1] + columnChunksMetadata[i-1].length;
      }
    }
    return offsets;
  }

  public static final class Reader implements Iterable<Bundle> {

    private final long numRecords;
    private final IColumnChunkReader[] columnChunkReaders;
    private final Schema.Column[] queriedColumns;
    private int bundleSize;

    public Reader(Types types, ByteBuffer bb, Metadata.RecordGroup recordGroupMetadata,
                  Schema.Column[] queriedColumns, int bundleSize) {
      this.numRecords = recordGroupMetadata.numRecords;
      this.queriedColumns = queriedColumns;
      this.columnChunkReaders = new IColumnChunkReader[queriedColumns.length];
      this.bundleSize = bundleSize;
      int[] columnChunksByteOffsets = getColumnChunkByteOffsets(recordGroupMetadata);
      Metadata.ColumnChunk[] columnChunksMetadata = recordGroupMetadata.columnChunks;
      for (int i=0; i<queriedColumns.length; ++i) {
        Schema.Column column = queriedColumns[i];
        int idx = column.columnIndex;
        columnChunkReaders[i]
          = ColumnChunks.createReader(types,
                                      Bytes.sliceAhead(bb, columnChunksByteOffsets[idx]),
                                      columnChunksMetadata[idx],
                                      column,
                                      bundleSize);
      }
    }

    public Iterator<Bundle> iterator() {
      if (numRecords == 0) {
        return Collections.<Bundle>emptyList().iterator();
      }
      final Bundle.Factory bundleFactory = new Bundle.Factory(queriedColumns);
      final int numColumns = queriedColumns.length;
      if (numColumns == 0) {
        final List[] emptyListArray = new List[]{};
        return new AReadOnlyIterator<Bundle>() {
          long remaining = numRecords;
          @Override
          public boolean hasNext() {
            return remaining > 0;
          }

          @Override
          public Bundle next() {
            if (remaining > bundleSize) {
              remaining -= bundleSize;
              return bundleFactory.create(bundleSize, emptyListArray);
            } else {
              Bundle bundle = bundleFactory.create((int)remaining, emptyListArray);
              remaining = 0;
              return bundle;
            }
          }
        };
      } else {
        final List<Iterator<List<Object>>> partitionedColumnIterators
          = new ArrayList<Iterator<List<Object>>>(numColumns);
        for (IColumnChunkReader ccr : columnChunkReaders) {
          partitionedColumnIterators.add(ccr.iterator());
        }
        return new AReadOnlyIterator<Bundle>() {
          @Override
          public boolean hasNext() {
            return partitionedColumnIterators.get(0).hasNext();
          }

          @Override
          public Bundle next() {
            List[] columnValues = new List[numColumns];
            int i = 0;
            for (Iterator<List<Object>> pci : partitionedColumnIterators) {
              columnValues[i] = pci.next();
              i += 1;
            }
            return bundleFactory.create(columnValues[0].size(), columnValues);
          }
        };
      }
    }

    public long getNumRecords() {
      return numRecords;
    }

    public List<Stats.ColumnChunk> getColumnChunkStats() {
      List<Stats.ColumnChunk> columnChunkStats = new ArrayList<Stats.ColumnChunk>(columnChunkReaders.length);
      for (IColumnChunkReader columnChunkReader : columnChunkReaders) {
        columnChunkStats.add(columnChunkReader.getStats());
      }
      return columnChunkStats;
    }
  }

}
