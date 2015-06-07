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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
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
    mos.write(writer.getHeader().getType());
    mos.write(writer);
  }

  public static Iterable<IPageHeader> getHeaders(final ByteBuffer bb, final int n) {
    return new Iterable<IPageHeader>() {
      @Override
        public Iterator<IPageHeader> iterator() {
        return new AReadOnlyIterator<IPageHeader>() {
          private int i = 0;
          private ByteBuffer byteBuffer = bb ;

          @Override
            public boolean hasNext() {
            return i < n;
          }

          @Override
            public IPageHeader next() {
            if (i == n) {
              throw new NoSuchElementException();
            }
            IPageHeader header = readHeader(byteBuffer);
            byteBuffer = Bytes.sliceAhead(byteBuffer, header.getBodyLength());
            i += 1;
            return header;
          }
        };
      }
    };
  }

  public static List<Stats.Page> getPagesStats(Iterable<IPageHeader> headers) {
    List<Stats.Page> pagesStats = new ArrayList<Stats.Page>();
    for (IPageHeader header : headers) {
      pagesStats.add(header.getStats());
    }
    return pagesStats;
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

  public static Iterable<DataPage.Reader>
    getDataPageReaders(final ByteBuffer bb, final int n, final int maxRepetitionLevel,
                       final int maxDefinitionLevel, final IDecoderFactory decoderFactory,
                       final IDecompressorFactory decompressorFactory) {
    return new Iterable<DataPage.Reader>() {
      @Override
      public Iterator<DataPage.Reader> iterator() {
        return new AReadOnlyIterator<DataPage.Reader>() {
          private int i = 0;
          private ByteBuffer byteBuffer = bb;

          @Override
            public boolean hasNext() {
            return i < n;
          }

          @Override
            public DataPage.Reader next() {
            if (i == n) {
              throw new NoSuchElementException();
            }
            DataPage.Reader reader = getDataPageReader(byteBuffer, maxRepetitionLevel, maxDefinitionLevel,
                                                       decoderFactory, decompressorFactory);
            byteBuffer = reader.getNextBuffer();
            i += 1;
            return reader;
          }
        };
      }
    };
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

  private static class ReadResult {
    final List<List<Object>> fullPartitions;
    final List<Object> unfinishedPartition;
    ReadResult(List<List<Object>> fullPartitions, List<Object> unfinishedPartition){
      this.fullPartitions = fullPartitions;
      this.unfinishedPartition = unfinishedPartition;
    }
  }

  private static ReadResult readAndPartitionDataPage(DataPage.Reader reader, List<Object> unfinishedPartition,
                                                     int partitionLength) {
    List<List<Object>> fullPartitions = new ArrayList<List<Object>>();
    List<Object> currentPartition = unfinishedPartition;
    for (Object o : reader) {
      currentPartition.add(o);
      if (currentPartition.size() == partitionLength) {
        fullPartitions.add(currentPartition);
        currentPartition = new ArrayList<Object>(partitionLength);
      }
    }
    return new ReadResult(fullPartitions, currentPartition);
  }

  private static Future<ReadResult> readAndPartitionDataPageFuture(final DataPage.Reader reader,
                                                                   final List<Object> unfinishedPartition,
                                                                   final int partitionLength) {
    return Agent.soloExecutor.submit(new Callable<ReadResult>() {
        public ReadResult call() {
          return readAndPartitionDataPage(reader, unfinishedPartition, partitionLength);
        }
      });
  }

  private static class FirstPageReadResult extends ReadResult {
    final Iterator<DataPage.Reader> pageIterator;
    final int partitionLength;
    FirstPageReadResult(List<List<Object>> fullPartitions, List<Object> unfinishedPartition,
                        Iterator<DataPage.Reader> pageIterator, int partitionLength){
      super(fullPartitions, unfinishedPartition);
      this.pageIterator = pageIterator;
      this.partitionLength = partitionLength;
    }
  }

  private static Future<FirstPageReadResult>
    readAndPartitionFirstDataPageFuture(final ByteBuffer bb, final int n, final int partitionLength,
                                        final int maxRepetitionLevel, final int maxDefinitionLevel,
                                        final IDecoderFactory decoderFactory,
                                        final IDecompressorFactory decompressorFactory) {
    return Agent.soloExecutor.submit(new Callable<FirstPageReadResult>() {
        public FirstPageReadResult call() {
          Iterator<DataPage.Reader> pageIterator
            = getDataPageReaders(bb, n, maxRepetitionLevel, maxDefinitionLevel,
                                 decoderFactory, decompressorFactory).iterator();
          ReadResult res = readAndPartitionDataPage(pageIterator.next(),
                                                    new ArrayList<Object>(partitionLength),
                                                    partitionLength);
          return new FirstPageReadResult(res.fullPartitions, res.unfinishedPartition, pageIterator,
                                         partitionLength);
        }
      });
  }

  private static Future<FirstPageReadResult>
    readAndPartitionFirstDataPageWithDictionaryFuture(final ByteBuffer bb, final int n,
                                                      final int partitionLength,
                                                      final int maxRepetitionLevel,
                                                      final int maxDefinitionLevel,
                                                      final IDecoderFactory dictDecoderFactory,
                                                      final IDecoderFactory indicesDecoderFactory,
                                                      final IDecompressorFactory decompressorFactory) {
    return Agent.soloExecutor.submit(new Callable<FirstPageReadResult>() {
        public FirstPageReadResult call() {
          DictionaryPage.Reader dictReader = getDictionaryPageReader(bb, dictDecoderFactory,
                                                                     decompressorFactory);
          Object[] dictionary = dictReader.read();
          IDecoderFactory dataDecoderFactory = new Dictionary.DecoderFactory(dictionary,
                                                                             indicesDecoderFactory,
                                                                             dictDecoderFactory);
          Iterator<DataPage.Reader> pageIterator
            = getDataPageReaders(dictReader.getNextBuffer(), n, maxRepetitionLevel, maxDefinitionLevel,
                                 dataDecoderFactory, null).iterator();
          ReadResult res = readAndPartitionDataPage(pageIterator.next(),
                                                    new ArrayList<Object>(partitionLength),
                                                    partitionLength);
          return new FirstPageReadResult(res.fullPartitions, res.unfinishedPartition, pageIterator,
                                         partitionLength);
        }
      });
  }

  private final static class PartitionedValuesIterator extends AReadOnlyIterator<List<Object>> {
    private Future<ReadResult> fut;
    private Future<FirstPageReadResult> firstFut;
    private Iterator<DataPage.Reader> pageIterator;
    private Iterator<List<Object>> fullPartitionsIterator;
    private int partitionLength;

    PartitionedValuesIterator(Future<FirstPageReadResult> firstFut) {
      this.firstFut = firstFut;
      this.fut = null;
      this.pageIterator = null;
      this.fullPartitionsIterator = null;
      this.partitionLength = 0;
    }

    @Override
    public boolean hasNext() {
      if (fullPartitionsIterator == null) {
        processFirstPage();
        return hasNext();
      } else if (fullPartitionsIterator.hasNext()) {
        return true;
      } else if (fut != null) {
        processNextPage();
        return hasNext();
      } else {
        return false;
      }
    }

    @Override
    public List<Object> next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return fullPartitionsIterator.next();
    }

    private void processFirstPage() {
      FirstPageReadResult res;
      try {
        res = firstFut.get();
        firstFut = null;
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
      pageIterator = res.pageIterator;
      List<List<Object>> fullPartitions = res.fullPartitions;
      partitionLength = res.partitionLength;
      if (pageIterator.hasNext()) {
        fut = readAndPartitionDataPageFuture(pageIterator.next(), res.unfinishedPartition, partitionLength);
      } else if (!res.unfinishedPartition.isEmpty()) {
        fullPartitions.add(res.unfinishedPartition);
      }
      fullPartitionsIterator = fullPartitions.iterator();
    }

    private void processNextPage() {
      ReadResult res;
      try {
        res = fut.get();
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
      List<List<Object>> fullPartitions = res.fullPartitions;
      if (pageIterator.hasNext()) {
        fut = readAndPartitionDataPageFuture(pageIterator.next(), res.unfinishedPartition, partitionLength);
      } else {
        fut = null;
        if (!res.unfinishedPartition.isEmpty()) {
          fullPartitions.add(res.unfinishedPartition);
        }
      }
      fullPartitionsIterator = fullPartitions.iterator();
    }
  }

  public static Iterable<List<Object>>
    readAndPartitionDataPages(final ByteBuffer bb, final int n, final int partitionLength,
                              final int maxRepetitionLevel, final int maxDefinitionLevel,
                              final IDecoderFactory decoderFactory,
                              final IDecompressorFactory decompressorFactory) {
    if (n == 0) {
      return Collections.emptyList();
    }
    return new Iterable<List<Object>>() {
      @Override
      public Iterator<List<Object>> iterator() {
        return new PartitionedValuesIterator(readAndPartitionFirstDataPageFuture(bb, n, partitionLength,
                                                                                 maxRepetitionLevel,
                                                                                 maxDefinitionLevel,
                                                                                 decoderFactory,
                                                                                 decompressorFactory));
      }
    };
  }

  public static Iterable<List<Object>>
    readAndPartitionDataPagesWithDictionary(final ByteBuffer bb, final int n, final int partitionLength,
                                            final int maxRepetitionLevel, final int maxDefinitionLevel,
                                            final IDecoderFactory dictDecoderFactory,
                                            final IDecoderFactory indicesDecoderFactory,
                                            final IDecompressorFactory decompressorFactory) {
    if (n == 0) {
      return Collections.emptyList();
    }
    return new Iterable<List<Object>>() {
      @Override
      public Iterator<List<Object>> iterator() {
        return new PartitionedValuesIterator(
            readAndPartitionFirstDataPageWithDictionaryFuture(bb, n,
                                                              partitionLength,
                                                              maxRepetitionLevel,
                                                              maxDefinitionLevel,
                                                              dictDecoderFactory,
                                                              indicesDecoderFactory,
                                                              decompressorFactory));
      }
    };
  }

}
