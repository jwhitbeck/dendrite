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

import clojure.lang.ArraySeq;
import clojure.lang.AFn;
import clojure.lang.Cons;
import clojure.lang.IFn;
import clojure.lang.IPersistentMap;
import clojure.lang.IPersistentCollection;
import clojure.lang.ITransientCollection;
import clojure.lang.ISeq;
import clojure.lang.LazySeq;
import clojure.lang.RT;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public final class RecordGroup {

  public final static int
    NONE = 0,
    ONLY_DEFAULT = 1,
    ALL = 2;

  public final static class Writer implements IOutputBuffer, IFileWriteable {

    final IColumnChunkWriter[] columnChunkWriters;
    int numRecords;
    boolean canOptimize;

    public Writer(Types types, Schema.Column[] columns, int targetDataPageLength,
                  int optimizationStrategy) {
      columnChunkWriters = new IColumnChunkWriter[columns.length];
      int numOptimizingColumnChunkWriters = 0;
      for (int i=0; i<columns.length; ++i) {
        if (optimizationStrategy == ALL || (optimizationStrategy == ONLY_DEFAULT
                                            && columns[i].encoding == Types.PLAIN
                                            && columns[i].compression == Types.NONE)) {
          columnChunkWriters[i] = OptimizingColumnChunkWriter.create(types, columns[i], targetDataPageLength);
          numOptimizingColumnChunkWriters += 1;
        } else {
          columnChunkWriters[i] = ColumnChunks.createWriter(types, columns[i], targetDataPageLength);
        }
      }
      canOptimize = numOptimizingColumnChunkWriters > 0;
      numRecords = 0;
    }

    public void write(Bundle bundle) {
      numRecords += bundle.count();
      Object[] writerValuesPairs = new Object[columnChunkWriters.length];
      for (int i=0; i<columnChunkWriters.length; ++i) {
        writerValuesPairs[i] = new WriterValuesPair(columnChunkWriters[i], bundle.columnValues[i]);
      }
      Utils.doAll(Utils.pmap(new AFn() {
          public Object invoke(Object o) {
            WriterValuesPair wvp = (WriterValuesPair)o;
            wvp.writer.write(wvp.values);
            return null;
          }
        }, ArraySeq.create(writerValuesPairs)));
    }

    public boolean canOptimize() {
      return canOptimize;
    }

    public void optimize(final IPersistentMap compressionThresholds) {
      if (canOptimize) {
        ITransientCollection optimizingColumnChunkwriters = ChunkedPersistentList.EMPTY.asTransient();
        for (int i=0; i<columnChunkWriters.length; ++i) {
          if (columnChunkWriters[i] instanceof OptimizingColumnChunkWriter) {
            optimizingColumnChunkwriters.conj(columnChunkWriters[i]);
          }
        }
        ISeq optimizedColumnChunkWriters = Utils.pmap(new AFn() {
            public Object invoke(Object o) {
              return ((OptimizingColumnChunkWriter)o).optimize(compressionThresholds);
            }
          }, RT.seq(optimizingColumnChunkwriters.persistent()));
        for (ISeq s = optimizedColumnChunkWriters; s != null; s = s.next()) {
          IColumnChunkWriter ccw = (IColumnChunkWriter)s.first();
          columnChunkWriters[ccw.column().columnIndex] = ccw;
        }
        canOptimize = false;
      }
    }

    public Schema.Column[] columns() {
      Schema.Column[] columns = new Schema.Column[columnChunkWriters.length];
      for (int i=0; i<columnChunkWriters.length; ++i) {
        columns[i] = columnChunkWriters[i].column();
      }
      return columns;
    }

    public int numRecords() {
      return numRecords;
    }

    public Metadata.RecordGroup metadata() {
      Metadata.ColumnChunk[] columnChunksMetadata = new Metadata.ColumnChunk[columnChunkWriters.length];
      for (int i=0; i<columnChunkWriters.length; ++i) {
        columnChunksMetadata[i] = columnChunkWriters[i].metadata();
      }
      return new Metadata.RecordGroup(length(), numRecords, columnChunksMetadata);
    }

    @Override
    public void reset() {
      numRecords = 0;
      for (int i=0; i<columnChunkWriters.length; ++i) {
        columnChunkWriters[i].reset();
      }
    }

    @Override
    public void finish() {
      Utils.doAll(Utils.pmap(new AFn() {
          public Object invoke(Object o)  {
            ((IColumnChunkWriter)o).finish();
            return null;
          }},
          ArraySeq.create((Object[])columnChunkWriters)));
    }

    @Override
    public int length() {
      int length = 0;
      for (int i=0; i<columnChunkWriters.length; ++i) {
        length += columnChunkWriters[i].length();
      }
      return length;
    }

    @Override
    public int estimatedLength() {
      int estimatedLength = 0;
      for (int i=0; i<columnChunkWriters.length; ++i) {
        estimatedLength += columnChunkWriters[i].estimatedLength();
      }
      return estimatedLength;
    }

    @Override
    public void writeTo(MemoryOutputStream mos) {
      finish();
      for (int i=0; i<columnChunkWriters.length; ++i) {
        mos.write(columnChunkWriters[i]);
      }
    }

    @Override
    public void writeTo(FileChannel fileChannel) throws IOException {
      finish();
      for (int i=0; i<columnChunkWriters.length; ++i) {
        fileChannel.write(columnChunkWriters[i].byteBuffer());
      }
    }

  }

  private final static class WriterValuesPair {
    IColumnChunkWriter writer;
    ChunkedPersistentList values;
    WriterValuesPair(IColumnChunkWriter writer, ChunkedPersistentList values) {
      this.writer = writer;
      this.values = values;
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

  public final static class Reader {

    final int numRecords;
    final IColumnChunkReader[] columnChunkReaders;

    public Reader(Types types, ByteBuffer bb, Metadata.RecordGroup recordGroupMetadata,
                  Schema.Column[] queriedColumns) {
      this.numRecords = recordGroupMetadata.numRecords;
      this.columnChunkReaders = new IColumnChunkReader[queriedColumns.length];
      int[] columnChunksByteOffsets = getColumnChunkByteOffsets(recordGroupMetadata);
      Metadata.ColumnChunk[] columnChunksMetadata = recordGroupMetadata.columnChunks;
      for (int i=0; i<queriedColumns.length; ++i) {
        Schema.Column column = queriedColumns[i];
        int idx = column.columnIndex;
        columnChunkReaders[i]
          = ColumnChunks.createReader(types,
                                      Bytes.sliceAhead(bb, columnChunksByteOffsets[idx]),
                                      columnChunksMetadata[idx],
                                      column);
      }
    }

    private ISeq readBundled(final ISeq[] partitionedPages) {
      return new LazySeq(new AFn(){
          public Object invoke() {
            if (RT.seq(partitionedPages[0]) == null) {
              return null;
            }
            ChunkedPersistentList[] columnValues = new ChunkedPersistentList[partitionedPages.length];
            for (int i=0; i<partitionedPages.length; ++i) {
              columnValues[i] = (ChunkedPersistentList)partitionedPages[i].first();
              partitionedPages[i] = partitionedPages[i].next();
            }
            return new Cons(new Bundle(columnValues), readBundled(partitionedPages));
            }
        });
    }

    public ISeq readBundled(int partitionLength) {
      if (numRecords == 0) {
        return null;
      }
      ISeq[] partitionedPages = new ISeq[columnChunkReaders.length];
      for (int i=0; i<columnChunkReaders.length; ++i) {
        partitionedPages[i] = columnChunkReaders[i].readPartitioned(partitionLength);
      }
      return readBundled(partitionedPages);
    }

    public IPersistentMap stats() {
      ITransientCollection columnChunkStats = ChunkedPersistentList.EMPTY.asTransient();
      for (int i=0; i<columnChunkReaders.length; ++i) {
        columnChunkStats.conj(columnChunkReaders[i].stats());
      }
      return Stats.recordGroupStats(numRecords, columnChunkStats.persistent());
    }
  }

}
