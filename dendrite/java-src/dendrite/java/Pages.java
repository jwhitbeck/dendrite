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
import clojure.lang.ArraySeq;
import clojure.lang.ASeq;
import clojure.lang.Cons;
import clojure.lang.IFn;
import clojure.lang.ISeq;
import clojure.lang.IPersistentCollection;
import clojure.lang.IPersistentMap;
import clojure.lang.ITransientCollection;
import clojure.lang.LazySeq;
import clojure.lang.RT;

import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

public final class Pages {

  public final static int
    DATA = 0,
    DICTIONARY = 1;

  public static IPageHeader readHeader(ByteBuffer bb) {
    int pageType = (int)bb.get();
    switch (pageType) {
    case DATA: return DataPage.Header.read(bb);
    case DICTIONARY: return DictionaryPage.Header.read(bb);
    default: throw new IllegalStateException(String.format("Unkown page type %d", pageType));
    }
  }

  public static void writeTo(MemoryOutputStream mos, IPageWriter writer) {
    mos.write(writer.header().type());
    mos.write(writer);
  }

  public static ISeq readHeaders(final ByteBuffer bb, final int n) {
    return new LazySeq(new AFn() {
        public Object invoke() {
          if (n == 0) {
            return null;
          } else {
            ByteBuffer byteBuffer = (ByteBuffer)bb;
            IPageHeader header = readHeader(byteBuffer);
            return new Cons(header, readHeaders(Bytes.sliceAhead(byteBuffer, header.bodyLength()), n-1));
          }
        }
      });
  }

  public static IPersistentCollection getPagesStats(ISeq headers) {
    ITransientCollection coll = ChunkedPersistentList.EMPTY.asTransient();
    for (ISeq s = headers; s != null; s = s.next()) {
      coll.conj(((IPageHeader)s.first()).stats());
    }
    return coll.persistent();
  }

  public static DataPage.Reader getDataPageReader(ByteBuffer bb, int maxRepetitionLevel,
                                                  int maxDefinitionLevel, IDecoderFactory decoderFactory,
                                                  IDecompressorFactory decompressorFactory) {
    ByteBuffer byteBuffer = bb.slice();
    int pageType = (int)byteBuffer.get();
    if (pageType != DATA) {
      throw new IllegalStateException(String.format("Page type %d is not a data page type.", pageType));
    }
    return DataPage.Reader.create(byteBuffer, maxRepetitionLevel, maxDefinitionLevel, decoderFactory,
                                  decompressorFactory);
  }

  public static ISeq getDataPageReaders(final ByteBuffer bb, final int n, final int maxRepetitionLevel,
                                        final int maxDefinitionLevel, final IDecoderFactory decoderFactory,
                                        final IDecompressorFactory decompressorFactory) {
    return new LazySeq(new AFn() {
        public Object invoke() {
          if (n == 0) {
            return null;
          } else {
            DataPage.Reader reader = getDataPageReader(bb, maxRepetitionLevel, maxDefinitionLevel,
                                                       decoderFactory, decompressorFactory);
            return new Cons(reader,
                            getDataPageReaders(reader.next(), n-1, maxRepetitionLevel,
                                               maxDefinitionLevel, decoderFactory, decompressorFactory));
          }
        }
      });
  }

  public static DictionaryPage.Reader getDictionaryPageReader(ByteBuffer bb, IDecoderFactory decoderFactory,
                                                              IDecompressorFactory decompressorFactory) {
    ByteBuffer byteBuffer = bb.slice();
    int pageType = (int)byteBuffer.get();
    if (pageType != DICTIONARY) {
      throw new IllegalStateException(String.format("Page type %d is not a dictionary page type.", pageType));
    }
    return DictionaryPage.Reader.create(byteBuffer, decoderFactory, decompressorFactory);
  }

  private final static class DataPageReadResult {
    final ISeq fullPartitions;
    final ISeq unfinishedPartition;
    final int unfinishedPartitionLength;
    final ISeq nextDataPageReaders;
    final int partitionLength;

    DataPageReadResult(IPersistentCollection fullPartitions, IPersistentCollection unfinishedPartition,
                       int unfinishedPartitionLength, ISeq nextDataPageReaders, int partitionLength) {
      this.fullPartitions = RT.seq(fullPartitions);
      this.unfinishedPartition = RT.seq(unfinishedPartition);
      this.unfinishedPartitionLength = unfinishedPartitionLength;
      this.nextDataPageReaders = nextDataPageReaders;
      this.partitionLength = partitionLength;
    }
  }

  static DataPageReadResult readAndPartitionDataPage(ISeq dataPageReaders, ISeq unfinishedPartition,
                                                     int unfinishedPartitionLength, int partitionLength) {
    DataPage.Reader reader = (DataPage.Reader)dataPageReaders.first();
    ChunkedPersistentList values = (ChunkedPersistentList)reader.read();
    ITransientCollection partitions = ChunkedPersistentList.EMPTY.asTransient();
    int numValues = (values == null)? 0 : values.count();
    if (unfinishedPartitionLength > 0) {
      if (unfinishedPartitionLength + numValues < partitionLength) {
        return new DataPageReadResult(null, Utils.concat(unfinishedPartition, values),
                                      unfinishedPartitionLength + numValues, dataPageReaders.next(),
                                      partitionLength);
      } else {
        int numToComplete = partitionLength - unfinishedPartitionLength;
        partitions.conj(Utils.concat(unfinishedPartition, values.take(numToComplete)));
        values = values.drop(numToComplete);
        numValues -= numToComplete;
      }
    }
    while (numValues >= partitionLength) {
      partitions.conj(values.take(partitionLength));
      values = values.drop(partitionLength);
      numValues -= partitionLength;
    }
    return new DataPageReadResult(partitions.persistent(), values, numValues, dataPageReaders.next(),
                                  partitionLength);
  }

  private static Future<DataPageReadResult>
    readAndPartitionDataPageFuture(final ISeq dataPageReaders, final ISeq unfinishedPartition,
                                   final int unfinishedPartitionLength, final int partitionLength) {
    return Agent.soloExecutor.submit(new Callable<DataPageReadResult>() {
        public DataPageReadResult call() {
          return readAndPartitionDataPage(dataPageReaders, unfinishedPartition, unfinishedPartitionLength,
                                          partitionLength);
        }
      });
  }

  private static Future<DataPageReadResult>
    readAndPartitionFirstDataPageFuture(final ByteBuffer bb, final int n, final int partitionLength,
                                        final int maxRepetitionLevel, final int maxDefinitionLevel,
                                        final IDecoderFactory decoderFactory,
                                        final IDecompressorFactory decompressorFactory) {

    return Agent.soloExecutor.submit(new Callable<DataPageReadResult>() {
        public DataPageReadResult call() {
          ISeq dataPageReaders = getDataPageReaders(bb, n, maxRepetitionLevel, maxDefinitionLevel,
                                                    decoderFactory, decompressorFactory);
          return readAndPartitionDataPage(dataPageReaders, null, 0, partitionLength);
        }
      });
  }

  private static Future<DataPageReadResult>
    readAndPartitionFirstDataPageWithDictionaryFuture(final ByteBuffer bb, final int n,
                                                      final int partitionLength,
                                                      final int maxRepetitionLevel,
                                                      final int maxDefinitionLevel,
                                                      final IDecoderFactory dictDecoderFactory,
                                                      final IDecoderFactory indicesDecoderFactory,
                                                      final IDecompressorFactory decompressorFactory) {
    return Agent.soloExecutor.submit(new Callable<DataPageReadResult>() {
        public DataPageReadResult call() {
          DictionaryPage.Reader dictReader = getDictionaryPageReader(bb, dictDecoderFactory,
                                                                     decompressorFactory);
          Object[] dictionary = dictReader.read();
          IDecoderFactory dataDecoderFactory = new Dictionary.DecoderFactory(dictionary,
                                                                             indicesDecoderFactory,
                                                                             dictDecoderFactory);
          ISeq dataPageReaders = getDataPageReaders(dictReader.next(), n, maxRepetitionLevel,
                                                    maxDefinitionLevel, dataDecoderFactory, null);
          return readAndPartitionDataPage(dataPageReaders, null, 0, partitionLength);
        }
      });
  }

  private final static class PartitionedDataPageSeq extends ASeq {

    final Future<DataPageReadResult> fut;
    DataPageReadResult res = null;
    PartitionedDataPageSeq next = null;

    PartitionedDataPageSeq(Future<DataPageReadResult> fut) {
      this.fut = fut;
    }

    PartitionedDataPageSeq(IPersistentMap meta, Future<DataPageReadResult> fut, DataPageReadResult res,
                           PartitionedDataPageSeq next) {
      super(meta);
      this.fut = fut;
      this.res = res;
      this.next = next;
    }

    @Override
    public PartitionedDataPageSeq withMeta(IPersistentMap meta) {
      return new PartitionedDataPageSeq(meta, fut, res, next);
    }

    private synchronized void step() {
      if (res == null) {
        try {
          res = fut.get();
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
        if (RT.seq(res.nextDataPageReaders) != null) {
          next = new PartitionedDataPageSeq(readAndPartitionDataPageFuture(res.nextDataPageReaders,
                                                                           res.unfinishedPartition,
                                                                           res.unfinishedPartitionLength,
                                                                           res.partitionLength));
        }
      }
    }

    @Override
    public Object first() {
      step();
      if (RT.seq(res.fullPartitions) != null) {
        return res.fullPartitions.first();
      } else if (next != null) {
        return next.first();
      } else {
        return res.unfinishedPartition;
      }
    }

    @Override
    public ISeq next() {
      step();
      if (next == null) {
        if (RT.seq(res.fullPartitions) == null) {
          return null;
        } else if (RT.seq(res.unfinishedPartition) != null) {
          return Utils.concat(res.fullPartitions.next(),
                              ArraySeq.create(new Object[]{res.unfinishedPartition}));
        } else {
          return res.fullPartitions.next();
        }
      } else if (RT.seq(res.fullPartitions) != null) {
        return Utils.concat(res.fullPartitions.next(), next);
      } else {
        return next.next();
      }
    }
  }

  public static ISeq readDataPagesPartitioned(ByteBuffer bb, int n, int partitionLength,
                                              int maxRepetitionLevel, int maxDefinitionLevel,
                                              IDecoderFactory decoderFactory,
                                              IDecompressorFactory decompressorFactory) {
    if (n == 0) {
      return null;
    }
    return new PartitionedDataPageSeq(readAndPartitionFirstDataPageFuture(bb, n, partitionLength,
                                                                          maxRepetitionLevel,
                                                                          maxDefinitionLevel,
                                                                          decoderFactory,
                                                                          decompressorFactory));
  }

  public static ISeq readDataPagesWithDictionaryPartitioned(ByteBuffer bb, int n, int partitionLength,
                                                            int maxRepetitionLevel, int maxDefinitionLevel,
                                                            IDecoderFactory dictDecoderFactory,
                                                            IDecoderFactory indicesDecoderFactory,
                                                            IDecompressorFactory decompressorFactory) {
    if (n == 0) {
      return null;
    }
    return new PartitionedDataPageSeq
      (readAndPartitionFirstDataPageWithDictionaryFuture(bb, n,
                                                         partitionLength,
                                                         maxRepetitionLevel,
                                                         maxDefinitionLevel,
                                                         dictDecoderFactory,
                                                         indicesDecoderFactory,
                                                         decompressorFactory));
  }

}
