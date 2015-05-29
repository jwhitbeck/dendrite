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

import clojure.lang.ArrayChunk;
import clojure.lang.AFn;
import clojure.lang.Agent;
import clojure.lang.ChunkedCons;
import clojure.lang.Cons;
import clojure.lang.Keyword;
import clojure.lang.IChunkedSeq;
import clojure.lang.IFn;
import clojure.lang.IPersistentCollection;
import clojure.lang.IPersistentMap;
import clojure.lang.IPersistentVector;
import clojure.lang.ISeq;
import clojure.lang.LazySeq;
import clojure.lang.PersistentArrayMap;
import clojure.lang.ITransientCollection;
import clojure.lang.Seqable;
import clojure.lang.Sequential;
import clojure.lang.RT;

import java.io.Closeable;
import java.io.IOException;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

public final class Reader implements Closeable {

  public final static Keyword
    RECORD_GROUPS = Keyword.intern("record-groups"),
    COLUMNS = Keyword.intern("columns"),
    GLOBAL = Keyword.intern("global");

  final FileChannel fileChannel;
  final Metadata.File fileMetadata;
  final Types types;

  private Reader(Types types, FileChannel fileChannel, Metadata.File fileMetadata) {
    this.types = types;
    this.fileChannel = fileChannel;
    this.fileMetadata = fileMetadata;
  }

  public static Reader create(Options.ReaderOptions options, File file) throws IOException {
    FileChannel fileChannel = Utils.getReadingFileChannel(file);
    Metadata.File fileMetadata = readMetadata(fileChannel);
    Types types = Types.create(options.customTypeDefinitions, fileMetadata.customTypes);
    return new Reader(types, fileChannel, fileMetadata);
  }

  public ByteBuffer getMetadata() {
    return fileMetadata.metadata;
  }

  View getView(Schema.QueryResult queryResult, int bundleSize) {
    Assemble.Fn assembleFn = Assemble.getFn(queryResult.schema);
    if (queryResult.columns.length == 0) {
      return new EmptyView(fileMetadata.recordGroups, assembleFn.invoke(null));
    } else {
      return new LazyView(this, queryResult.columns, assembleFn, bundleSize);
    }
  }

  Schema.QueryResult getQueryResult(Options.ReadOptions options, IFn pmapFn) {
    Schema.QueryResult res = Schema.applyQuery(types,
                                               options.isMissingFieldsAsNil,
                                               options.readers,
                                               Schema.subSchema(options.entrypoint, fileMetadata.schema),
                                               options.query);
    if (pmapFn != null) {
      IFn fn = res.schema.fn;
      if (fn == null) {
        fn = pmapFn;
      } else {
        fn = Utils.comp(pmapFn, fn);
      }
      return new Schema.QueryResult(res.schema.withFn(fn), res.columns);
    } else {
      return res;
    }
  }

  public View read(Options.ReadOptions options) {
    return getView(getQueryResult(options, null), options.bundleSize);
  }

  public View pmap(Options.ReadOptions options, IFn pmapFn) {
    return getView(getQueryResult(options, pmapFn), options.bundleSize);
  }

  Iterator<RecordGroup.Reader> getRecordGroupReaders(Schema.Column[] columns, int bundleSize) {
    return getRecordGroupReaders(types, fileChannel, Constants.magicBytes.length,
                                 fileMetadata.recordGroups, columns, bundleSize);
  }

  private static Iterator<RecordGroup.Reader>
    getRecordGroupReaders(final Types types, final FileChannel fileChannel, final long offset,
                          final Metadata.RecordGroup[] recordGroupsMetadata,
                          final Schema.Column[] queriedColumns, final int bundleSize) {
    final int numRecordGroups = recordGroupsMetadata.length;
    if (numRecordGroups == 0) {
      return Collections.<RecordGroup.Reader>emptyList().iterator();
    }
    return new AReadOnlyIterator<RecordGroup.Reader>() {
      int i = 0;
      long nextOffset = offset;

      @Override
      public boolean hasNext() {
        return i < numRecordGroups;
      }

      @Override
      public RecordGroup.Reader next() {
        Metadata.RecordGroup recordGroupMetadata = recordGroupsMetadata[i];
        long length = recordGroupMetadata.length;
        ByteBuffer bb;
        try {
          bb = Utils.mapFileChannel(fileChannel, nextOffset, length);
        } catch (IOException e) {
          throw new IllegalStateException(e);
        }
        RecordGroup.Reader recordGroupReader
          = new RecordGroup.Reader(types, bb, recordGroupMetadata, queriedColumns, bundleSize);
        nextOffset += length;
        i += 1;
        return recordGroupReader;
      }
    };
  }

  public IPersistentMap stats() throws IOException {
    // TODO Fix
    // IPersistentVector[] paths = Schema.getPaths(fileMetadata.schema);
    // Schema.Column[] columns = Schema.getColumns(fileMetadata.schema);
    // ITransientCollection[] columnStats = new ITransientCollection[columns.length];
    // ITransientCollection recordGroupStats = ChunkedPersistentList.EMPTY.asTransient();
    // for (int i=0; i<columns.length; ++i) {
    //   columnStats[i] = ChunkedPersistentList.EMPTY.asTransient();
    // }
    // for (ISeq s = RT.seq(getRecordGroupReaders(columns)); s != null; s = s.next()) {
    //   RecordGroup.Reader recordGroupReader = (RecordGroup.Reader)s.first();
    //   IPersistentMap[] columnChunkStats = recordGroupReader.columnChunkStats();
    //   for (int i=0; i<columnChunkStats.length; ++i) {
    //     columnStats[i].conj(columnChunkStats[i]);
    //   }
    //   recordGroupStats.conj(Stats.recordGroupStats(recordGroupReader.numRecords(), RT.seq(columnChunkStats)));
    // }
    // IPersistentCollection[] finalColumnStats = new IPersistentCollection[columnStats.length];
    // for (int i=0; i<columnStats.length; ++i) {
    //   Schema.Column col = columns[i];
    //   finalColumnStats[i] = Stats.columnStats(types.getTypeSymbol(col.type),
    //                                           types.getEncodingSymbol(col.encoding),
    //                                           types.getCompressionSymbol(col.compression),
    //                                           col.repetitionLevel,
    //                                           col.definitionLevel,
    //                                           paths[i],
    //                                           columnStats[i].persistent());
    // }
    // IPersistentCollection finalRecordGroupStats = recordGroupStats.persistent();
    // return new PersistentArrayMap(new Object[]{
    //     RECORD_GROUPS, recordGroupStats.persistent(),
    //     COLUMNS, RT.seq(finalColumnStats),
    //     GLOBAL, Stats.globalStats(fileChannel.size(), columns.length, finalRecordGroupStats)
    //   });
    return null;
  }

  public Object schema() {
    return Schema.unparse(types, fileMetadata.schema);
  }

  @Override
  public void close() throws IOException {
    fileChannel.close();
  }

  private static boolean isValidMagicBytes(ByteBuffer bb) {
    return Arrays.equals(Constants.magicBytes, Types.toByteArray(bb));
  }

  private static final int fixedIntLength = 4;

  static Metadata.File readMetadata(FileChannel fileChannel) throws IOException {
    long length = fileChannel.size();
    long lastMagicBytesPosition = length - Constants.magicBytes.length;
    ByteBuffer lastMagicBytesBuffer
      = Utils.mapFileChannel(fileChannel, lastMagicBytesPosition, Constants.magicBytes.length);
    if (!isValidMagicBytes(lastMagicBytesBuffer)) {
      throw new IllegalStateException("File is not a valid dendrite file.");
    }
    long metadataLengthPosition = lastMagicBytesPosition - fixedIntLength;
    if (metadataLengthPosition < Constants.magicBytes.length) {
      throw new IllegalStateException("File is not a valid dendrite file.");
    }
    ByteBuffer metadataLengthBuffer
      = Utils.mapFileChannel(fileChannel, metadataLengthPosition, fixedIntLength);
    metadataLengthBuffer.order(ByteOrder.LITTLE_ENDIAN);
    long metadataLength = metadataLengthBuffer.getInt();
    if (metadataLength <= 0) {
      throw new IllegalStateException("File is not a valid dendrite file.");
    }
    ByteBuffer metadataBuffer
      = Utils.mapFileChannel(fileChannel, metadataLengthPosition - metadataLength, metadataLength);
    return Metadata.File.read(metadataBuffer);
  }

  public static final class EmptyView extends View {

    final Metadata.RecordGroup[] recordGroupsMetadata;
    final Object assembledNilRecord;

    EmptyView(Metadata.RecordGroup[] recordGroupsMetadata, Object assembledNilRecord) {
      this.recordGroupsMetadata = recordGroupsMetadata;
      this.assembledNilRecord = assembledNilRecord;
    }

    @Override
    public ISeq seq() {
      long totalNumRecords = 0;
      for (int i=0; i<recordGroupsMetadata.length; ++i) {
        totalNumRecords = recordGroupsMetadata[i].numRecords;
      }
      return Utils.repeat(totalNumRecords, assembledNilRecord);
    }

    Object reduce(long n, IFn reducef, Object init) {
      Object ret = init;
      for (long i=0; i<n; ++i) {
        ret = reducef.invoke(ret, assembledNilRecord);
      }
      return ret;
    }

    @Override
    public Object fold(int n, IFn combinef, IFn reducef) {
      Object init = combinef.invoke();
      Object foldedBundle = reduce(n, reducef, init);
      Object ret = init;
      for (int i=0; i<recordGroupsMetadata.length; ++i) {
        long numRecords = recordGroupsMetadata[i].numRecords;
        for (long j=0; j<(numRecords/n); ++j) {
          ret = combinef.invoke(ret, foldedBundle);
        }
        ret = combinef.invoke(ret, reduce(numRecords % n, reducef, init));
      }
      return ret;
    }

  }

  public static final class LazyView extends View {

    private final Assemble.Fn assembleFn;
    private final Schema.Column[] queriedColumns;
    private final Reader reader;
    private final int defaultBundleSize;
    private ISeq recordSeq = null;

    LazyView(Reader reader, Schema.Column[] queriedColumns, Assemble.Fn assembleFn, int defaultBundleSize) {
      this.reader = reader;
      this.queriedColumns = queriedColumns;
      this.assembleFn = assembleFn;
      this.defaultBundleSize = defaultBundleSize;
    }

    private static Iterator<Bundle> getBundlesIterator(final Iterator<RecordGroup.Reader> recordGroupReaders) {
      if (!recordGroupReaders.hasNext()) {
        return Collections.<Bundle>emptyList().iterator();
      }
      return new AReadOnlyIterator<Bundle>() {
        private Iterator<Bundle> bundleIterator = recordGroupReaders.next().iterator();

        private void step() {
          if (recordGroupReaders.hasNext()) {
            bundleIterator = recordGroupReaders.next().iterator();
          } else {
            bundleIterator = null;
          }
        }

        @Override
        public boolean hasNext() {
          if (bundleIterator == null) {
            return false;
          }
          if (bundleIterator.hasNext()) {
            return true;
          } else {
            step();
            return hasNext();
          }
        }

        @Override
        public Bundle next() {
          if (!hasNext()) {
            throw new NoSuchElementException();
          }
          return bundleIterator.next();
        }
      };
    }

    private Iterator<Bundle> getBundlesIterator(int bundleSize) {
      return getBundlesIterator(reader.getRecordGroupReaders(queriedColumns, bundleSize));
    }

    private static Future<ArrayChunk> getAssembleFuture(final Bundle bundle, final Assemble.Fn assembleFn) {
      return Agent.soloExecutor.submit(new Callable<ArrayChunk>() {
          public ArrayChunk call() {
                return new ArrayChunk(bundle.assemble(assembleFn));
          }
        });
    }

    private static Iterator<ArrayChunk> getRecordChunksIterator(final Iterator<Bundle> bundlesIterator,
                                                                final Assemble.Fn assembleFn) {
      int n = 2 + Runtime.getRuntime().availableProcessors();
      final LinkedList<Future<ArrayChunk>> futures = new LinkedList<Future<ArrayChunk>>();
      int k = 0;
      while (bundlesIterator.hasNext() && k < n) {
        futures.addLast(getAssembleFuture(bundlesIterator.next(), assembleFn));
      }
      return new AReadOnlyIterator<ArrayChunk>() {
        @Override
        public boolean hasNext() {
          return !futures.isEmpty();
        }

        @Override
        public ArrayChunk next() {
          Future<ArrayChunk> fut = futures.pollFirst();
          ArrayChunk chunk = Utils.tryGetFuture(fut);
          if (bundlesIterator.hasNext()) {
            futures.addLast(getAssembleFuture(bundlesIterator.next(), assembleFn));
          }
          return chunk;
        }
      };
    }

    private Iterator<ArrayChunk> getRecordChunksIterator(int bundleSize) {
      return getRecordChunksIterator(getBundlesIterator(bundleSize), assembleFn);
    }

    private static ISeq getRecordChunkedSeq(final Iterator<ArrayChunk> recordChunksIterator) {
      return new LazySeq(new AFn() {
          public IChunkedSeq invoke() {
            if (!recordChunksIterator.hasNext()) {
              return null;
            } else {
              return new ChunkedCons(recordChunksIterator.next(), getRecordChunkedSeq(recordChunksIterator));
            }
          }
        });
    }

    private ISeq getRecordChunkedSeq(int bundleSize) {
      return getRecordChunkedSeq(getRecordChunksIterator(bundleSize));
    }

    @Override
    public synchronized ISeq seq() {
      if (recordSeq == null) {
        recordSeq = RT.seq(getRecordChunkedSeq(defaultBundleSize));
      }
      return recordSeq;
    }

    @Override
    public synchronized Object fold(final int n, final IFn combinef, final IFn reducef) {
      throw new UnsupportedOperationException();
      // final Object init = combinef.invoke();
      // if (assembledBundleseq == null) {
      //   IFn fn = new AFn() {
      //       public Object invoke(Object bundle) {
      //         return ((ReadBundle)bundle).reduce(reducef, assembleFn, init);
      //       }
      //     };
      //   return reduce(combinef, init, Utils.pmap(fn, getBundlesSeq(n)));
      // } else {
      //   IFn fn = new AFn() {
      //       public Object invoke(Object assembledBundle) {
      //         return reduce(reducef, init, assembledBundle);
      //       }
      //     };
      //   return reduce(reducef, init, Utils.pmap(fn, assembledBundleseq));
      // }
    }
  }
}
