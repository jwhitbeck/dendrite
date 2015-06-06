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

import java.io.Closeable;
import java.io.IOException;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

public final class FileWriter implements Closeable {

  private final Types types;
  private final Schema schema;
  private final FileChannel fileChannel;
  private ByteBuffer metadata;
  private List<Object> batchBuffer;
  private int numBufferedRecords;
  private final LinkedBlockingQueue<List<Object>> batchQueue;
  private final Future<WriteThreadResult> writeThread;
  private final int bundleSize;
  private boolean isClosed;

  private FileWriter(Types types, Schema schema, FileChannel fileChannel, int bundleSize,
                     Future<WriteThreadResult> writeThread, LinkedBlockingQueue<List<Object>> batchQueue) {
    this.types = types;
    this.schema = schema;
    this.fileChannel = fileChannel;
    this.bundleSize = bundleSize;
    this.metadata = null;
    this.batchBuffer = new ArrayList<Object>(bundleSize);
    this.numBufferedRecords = 0;
    this.batchQueue = batchQueue;
    this.writeThread = writeThread;
    this.isClosed = false;
  }

  public static FileWriter create(Options.WriterOptions writerOptions, Object unparsedSchema, File file)
    throws IOException {
    Types types = Types.create(writerOptions.customTypeDefinitions);
    Schema schema = Schema.parse(types, unparsedSchema);
    Schema.Column[] columns = Schema.getColumns(schema);
    RecordGroup.Writer recordGroupWriter = new RecordGroup.Writer(types, columns, writerOptions.dataPageLength,
                                                                  writerOptions.optimizationStrategy);
    Stripe.Fn stripeFn = Stripe.getFn(types, schema, writerOptions.mapFn, writerOptions.invalidInputHandler);
    LinkedBlockingQueue<List<Object>> batchQueue = new LinkedBlockingQueue<List<Object>>(100);
    Bundle.Factory bundleFactory = new Bundle.Factory(columns);
    FileChannel fileChannel = Utils.getWritingFileChannel(file);
    fileChannel.write(ByteBuffer.wrap(Constants.magicBytes));
    Future<WriteThreadResult> writeThread = startWriteThread(recordGroupWriter,
                                                             bundleFactory,
                                                             stripeFn,
                                                             fileChannel,
                                                             writerOptions.recordGroupLength,
                                                             writerOptions.bundleSize,
                                                             writerOptions.compressionThresholds,
                                                             getBatchIterator(batchQueue));
    return new FileWriter(types, schema, fileChannel, writerOptions.bundleSize, writeThread, batchQueue);
  }

  private final static List<Object> poison = new ArrayList<Object>();

  private static Iterator<List<Object>> getBatchIterator(final LinkedBlockingQueue<List<Object>> batchQueue) {
    return new AReadOnlyIterator<List<Object>>() {
      private List<Object> next = null;

      private void step() {
        try {
          next = batchQueue.take();
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
      }

      @Override
      public boolean hasNext() {
        if (next == null) {
          step();
          return hasNext();
        } else if (next == poison) {
          return false;
        } else {
          return true;
        }
      }

      @Override
      public List<Object> next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        List<Object> ret = next;
        step();
        return ret;
      }
    };
  }

  private static Future<Bundle> getBundleFuture(final Bundle.Factory bundleFactory,
                                                final Stripe.Fn stripeFn,
                                                final List<Object> records) {
    return Agent.soloExecutor.submit(new Callable<Bundle>() {
        public Bundle call() {
          return bundleFactory.stripe(stripeFn, records);
        }
      });
  }

  private static Iterator<Bundle> getBundleIterator(final Bundle.Factory bundleFactory,
                                                    final Stripe.Fn stripeFn,
                                                    final Iterator<List<Object>> batchIterator) {
    int n = 2 + Runtime.getRuntime().availableProcessors();
    final LinkedList<Future<Bundle>> futures = new LinkedList<Future<Bundle>>();
    int k = 0;
    while (batchIterator.hasNext() && k < n) {
      futures.addLast(getBundleFuture(bundleFactory, stripeFn, batchIterator.next()));
    }
    return new AReadOnlyIterator<Bundle>() {
      @Override
      public boolean hasNext() {
        return !futures.isEmpty();
      }

      @Override
      public Bundle next() {
        Future<Bundle> fut = futures.pollFirst();
        Bundle bundle = Utils.tryGetFuture(fut);
        if (batchIterator.hasNext()) {
          futures.addLast(getBundleFuture(bundleFactory, stripeFn, batchIterator.next()));
        }
        return bundle;
      }
    };
  }

  static final class WriteThreadResult {
    final Metadata.RecordGroup[] recordGroupsMetadata;
    final Schema.Column[] columns;
    WriteThreadResult(Metadata.RecordGroup[] recordGroupsMetadata, Schema.Column[] columns) {
      this.recordGroupsMetadata = recordGroupsMetadata;
      this.columns = columns;
    }
  }

  static Future<WriteThreadResult>
    startWriteThread(final RecordGroup.Writer recordGroupWriter,
                     final Bundle.Factory bundleFactory,
                     final Stripe.Fn stripeFn,
                     final FileChannel fileChannel,
                     final int targetRecordGroupLength,
                     final int bundleSize,
                     final Map<Symbol,Double> compressionThresholds,
                     final Iterator<List<Object>> batchIterator) {
    return Agent.soloExecutor.submit(new Callable<WriteThreadResult>() {
        public WriteThreadResult call() throws IOException {
          ArrayList<Metadata.RecordGroup> recordGroupsMetadata = new ArrayList<Metadata.RecordGroup>();
          Iterator<Bundle> bundleIterator = getBundleIterator(bundleFactory, stripeFn, batchIterator);
          long nextNumRecordsForLengthCheck = 10L * bundleSize;
          while (bundleIterator.hasNext()) {
            Bundle bundle = bundleIterator.next();
            while (true) {
              long currentNumRecords = recordGroupWriter.numRecords();
              if (currentNumRecords >= nextNumRecordsForLengthCheck) {
                int estimatedLength = recordGroupWriter.estimatedLength();
                if (estimatedLength >= targetRecordGroupLength) {
                  if (recordGroupWriter.canOptimize()) {
                    recordGroupWriter.optimize(compressionThresholds);
                  } else {
                    recordGroupWriter.finish();
                    recordGroupsMetadata.add(recordGroupWriter.metadata());
                    recordGroupWriter.writeTo(fileChannel);
                    recordGroupWriter.reset();
                    nextNumRecordsForLengthCheck = currentNumRecords / 2;
                  }
                } else {
                  nextNumRecordsForLengthCheck = Thresholds.nextCheckThreshold(currentNumRecords,
                                                                               estimatedLength,
                                                                               targetRecordGroupLength);
                }
              } else {
                long remainingRecordsBeforeCheck = nextNumRecordsForLengthCheck - currentNumRecords;
                if (bundle.size() <= remainingRecordsBeforeCheck) {
                  recordGroupWriter.write(bundle);
                  break;
                } else {
                  recordGroupWriter.write(bundle.take((int)remainingRecordsBeforeCheck));
                  bundle = bundle.drop((int)remainingRecordsBeforeCheck);
                }
              }
            }
          }
          if (recordGroupWriter.numRecords() > 0) {
            if (recordGroupWriter.canOptimize()) {
              recordGroupWriter.optimize(compressionThresholds);
            }
            recordGroupWriter.finish();
            recordGroupsMetadata.add(recordGroupWriter.metadata());
            recordGroupWriter.writeTo(fileChannel);
          }
          return new WriteThreadResult(recordGroupsMetadata.toArray(new Metadata.RecordGroup[]{}),
                                       recordGroupWriter.columns());
        }
      });
  }

  public void setMetadata(ByteBuffer metadata) {
    this.metadata = metadata;
  }

  void flushBatch() {
    if (writeThread.isDone()) {
      // If the writeThread finished while we are still writing, it likely means it threw an exception.
      try {
        writeThread.get();
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    } else if (isClosed) {
      throw new IllegalStateException("Cannot write to a closed writer.");
    } else {
      try {
        batchQueue.put(batchBuffer);
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
      batchBuffer = new ArrayList<Object>(bundleSize);
      numBufferedRecords = 0;
    }
  }

  void writeFooter(Metadata.File fileMetadata) throws IOException {
    MemoryOutputStream mos = new MemoryOutputStream();
    mos.write(fileMetadata);
    Bytes.writeFixedInt(mos, mos.length());
    mos.write(Constants.magicBytes);
    fileChannel.write(mos.byteBuffer());
  }

  public void write(Object record) {
    if (numBufferedRecords == bundleSize) {
      flushBatch();
    }
    numBufferedRecords += 1;
    batchBuffer.add(record);
  }

  public void writeAll(List<Object> records) {
    for (Object o : records) {
      write(o);
    }
  }

  @Override
  public void close() throws IOException {
    if (!isClosed) {
      try {
        if (numBufferedRecords > 0) {
          flushBatch();
        }
        try {
          batchQueue.put(poison);
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
        WriteThreadResult res;
        try {
          res = writeThread.get();
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
        writeFooter(new Metadata.File(res.recordGroupsMetadata,
                                      schema.withColumns(res.columns),
                                      types.getCustomTypes(),
                                      metadata));
      } finally {
        fileChannel.close();
        isClosed = true;
      }
    }
  }
}
