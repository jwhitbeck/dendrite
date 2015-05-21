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

import clojure.lang.AFn;
import clojure.lang.Agent;
import clojure.lang.Cons;
import clojure.lang.LazySeq;
import clojure.lang.IFn;
import clojure.lang.IPersistentCollection;
import clojure.lang.IPersistentMap;
import clojure.lang.ISeq;
import clojure.lang.ITransientCollection;
import clojure.lang.RT;

import java.io.Closeable;
import java.io.IOException;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

public final class Writer implements Closeable {

  final static IPersistentCollection poison = new IPersistentCollection() {
      public int count() { throw new UnsupportedOperationException(); }
      public IPersistentCollection cons(Object o) { throw new UnsupportedOperationException(); }
      public IPersistentCollection empty() { throw new UnsupportedOperationException(); }
      public boolean equiv(Object o) { throw new UnsupportedOperationException(); }
      public ISeq seq() { throw new UnsupportedOperationException(); }
    };

  static ISeq blockingSeq(final LinkedBlockingQueue queue) {
    return new LazySeq(new AFn(){
        public Object invoke() {
          try {
            Object v = queue.take();
            if (v != poison) {
              return new Cons(v, blockingSeq(queue));
            } else {
              return null;
            }
          } catch (Exception e) {
            throw new IllegalStateException(e);
          }
        }
      });
  }

  static final class WriteThreadResult {
    final Metadata.RecordGroup[] recordGroupsMetadata;
    final Schema.Column[] columns;
    WriteThreadResult(Metadata.RecordGroup[] recordGroupsMetadata, Schema.Column[] columns) {
      this.recordGroupsMetadata = recordGroupsMetadata;
      this.columns = columns;
    }
  }

  static IFn getBundleStripeFn(final Stripe.Fn stripeFn, final int numColumns) {
    return new AFn() {
      public Bundle invoke(Object batch) {
        return Bundle.stripe(batch, stripeFn, numColumns);
      }
    };
  }

  static Future<WriteThreadResult>
    startWriteThread(final RecordGroup.Writer recordGroupWriter,
                     final IFn bundleStripeFn,
                     final FileChannel fileChannel,
                     final int targetRecordGroupLength,
                     final int bundleSize,
                     final IPersistentMap compressionThresholds,
                     final LinkedBlockingQueue<IPersistentCollection> batchQueue) {
    return Agent.soloExecutor.submit(new Callable<WriteThreadResult>() {
        public WriteThreadResult call() throws IOException {
          ISeq bundles = Utils.pmap(bundleStripeFn, blockingSeq(batchQueue));
          ArrayList<Metadata.RecordGroup> recordGroupsMetadata = new ArrayList<Metadata.RecordGroup>();
          int nextNumRecordsForLengthCheck = 10 * bundleSize;
          while (bundles != null) {
            Bundle bundle = (Bundle)bundles.first();
            while (bundle != null) {
              int currentNumRecords = recordGroupWriter.numRecords();
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
                int remainingRecordsBeforeCheck = nextNumRecordsForLengthCheck - currentNumRecords;
                if (bundle.count() <= remainingRecordsBeforeCheck) {
                  recordGroupWriter.write(bundle);
                  bundle = null;
                } else {
                  recordGroupWriter.write(bundle.take(remainingRecordsBeforeCheck));
                  bundle = bundle.drop(remainingRecordsBeforeCheck);
                }
              }
            }
            bundles = bundles.next();
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

  final Types types;
  final Schema schema;
  final FileChannel fileChannel;
  ByteBuffer customMetadata;
  ITransientCollection batchBuffer;
  int numBufferedRecords;
  final LinkedBlockingQueue<IPersistentCollection> batchQueue;
  final Future<WriteThreadResult> writeThread;
  final int bundleSize;
  boolean isClosed;

  Writer(Types types, Schema schema, FileChannel fileChannel, int bundleSize,
         Future<WriteThreadResult> writeThread, LinkedBlockingQueue<IPersistentCollection> batchQueue) {
    this.types = types;
    this.schema = schema;
    this.fileChannel = fileChannel;
    this.bundleSize = bundleSize;
    this.customMetadata = null;
    this.batchBuffer = ChunkedPersistentList.EMPTY.asTransient();
    this.numBufferedRecords = 0;
    this.batchQueue = batchQueue;
    this.writeThread = writeThread;
    this.isClosed = false;
  }

  public static Writer create(Options.WriterOptions writerOptions, Object unparsedSchema, File file)
    throws IOException {
    Types types = Types.create(writerOptions.customTypeDefinitions);
    Schema schema = Schema.parse(types, unparsedSchema);
    Schema.Column[] columns = Schema.getColumns(schema);
    RecordGroup.Writer recordGroupWriter = new RecordGroup.Writer(types, columns, writerOptions.dataPageLength,
                                                                  writerOptions.optimizationStrategy);
    Stripe.Fn stripeFn = Stripe.getFn(types, schema, writerOptions.invalidInputHandler);
    LinkedBlockingQueue<IPersistentCollection> batchQueue
      = new LinkedBlockingQueue<IPersistentCollection>(100);
    FileChannel fileChannel = Utils.getWritingFileChannel(file);
    fileChannel.write(ByteBuffer.wrap(Constants.magicBytes));
    Future<WriteThreadResult> writeThread = startWriteThread(recordGroupWriter,
                                                             getBundleStripeFn(stripeFn, columns.length),
                                                             fileChannel,
                                                             writerOptions.recordGroupLength,
                                                             writerOptions.bundleSize,
                                                             writerOptions.compressionThresholds,
                                                             batchQueue);
    return new Writer(types, schema, fileChannel, writerOptions.bundleSize, writeThread, batchQueue);
  }

  public void setCustomMetadata(ByteBuffer customMetadata) {
    this.customMetadata = customMetadata;
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
        batchQueue.put(batchBuffer.persistent());
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
      batchBuffer = ChunkedPersistentList.EMPTY.asTransient();
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
    batchBuffer.conj(record);
  }

  public void writeAll(Object records) {
    for (ISeq s = RT.seq(records); s != null; s = s.next()) {
      write(s.first());
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
                                      customMetadata));
      } finally {
        fileChannel.close();
        isClosed = true;
      }
    }
  }
}
