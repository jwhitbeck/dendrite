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

  Reader(Types types, FileChannel fileChannel, Metadata.File fileMetadata) {
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

  ISeq getRecordGroupReaders(Schema.Column[] columns) {
    return getRecordGroupReaders(types, fileChannel, Constants.magicBytes.length,
                                 RT.seq(fileMetadata.recordGroups), columns);
  }

  public IPersistentMap stats() throws IOException {
    IPersistentVector[] paths = Schema.getPaths(fileMetadata.schema);
    Schema.Column[] columns = Schema.getColumns(fileMetadata.schema);
    ITransientCollection[] columnStats = new ITransientCollection[columns.length];
    ITransientCollection recordGroupStats = ChunkedPersistentList.EMPTY.asTransient();
    for (int i=0; i<columns.length; ++i) {
      columnStats[i] = ChunkedPersistentList.EMPTY.asTransient();
    }
    for (ISeq s = RT.seq(getRecordGroupReaders(columns)); s != null; s = s.next()) {
      RecordGroup.Reader recordGroupReader = (RecordGroup.Reader)s.first();
      IPersistentMap[] columnChunkStats = recordGroupReader.columnChunkStats();
      for (int i=0; i<columnChunkStats.length; ++i) {
        columnStats[i].conj(columnChunkStats[i]);
      }
      recordGroupStats.conj(Stats.recordGroupStats(recordGroupReader.numRecords(), RT.seq(columnChunkStats)));
    }
    IPersistentCollection[] finalColumnStats = new IPersistentCollection[columnStats.length];
    for (int i=0; i<columnStats.length; ++i) {
      Schema.Column col = columns[i];
      finalColumnStats[i] = Stats.columnStats(types.getTypeSymbol(col.type),
                                              types.getEncodingSymbol(col.encoding),
                                              types.getCompressionSymbol(col.compression),
                                              col.repetitionLevel,
                                              col.definitionLevel,
                                              paths[i],
                                              columnStats[i].persistent());
    }
    IPersistentCollection finalRecordGroupStats = recordGroupStats.persistent();
    return new PersistentArrayMap(new Object[]{
        RECORD_GROUPS, recordGroupStats.persistent(),
        COLUMNS, RT.seq(finalColumnStats),
        GLOBAL, Stats.globalStats(fileChannel.size(), columns.length, finalRecordGroupStats)
      });
  }

  public Object schema() {
    return Schema.unparse(types, fileMetadata.schema);
  }

  @Override
  public void close() throws IOException {
    fileChannel.close();
  }

  static ISeq getRecordGroupReaders(final Types types, final FileChannel fileChannel, final long offset,
                                    final ISeq recordGroupsMetadata, final Schema.Column[] queriedColumns) {
    return new LazySeq(new AFn() {
        public ISeq invoke() {
          if (RT.seq(recordGroupsMetadata) == null) {
            return null;
          }
          Metadata.RecordGroup recordGroupMetadata = (Metadata.RecordGroup)recordGroupsMetadata.first();
          long length = recordGroupMetadata.length;
          ByteBuffer bb;
          try {
            bb = Utils.mapFileChannel(fileChannel, offset, length);
          } catch (IOException e) {
            throw new IllegalStateException(e);
          }
          RecordGroup.Reader recordGroupReader
            = new RecordGroup.Reader(types, bb, recordGroupMetadata, queriedColumns);
          return new Cons(recordGroupReader,
                          getRecordGroupReaders(types, fileChannel, offset + length,
                                                recordGroupsMetadata.next(), queriedColumns));
        }
      });
  }

  static boolean isValidMagicBytes(ByteBuffer bb) {
    return Arrays.equals(Constants.magicBytes, Types.toByteArray(bb));
  }

  static final int fixedIntLength = 4;

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
      return repeat(totalNumRecords, assembledNilRecord);
    }

    Object reduce(int n, IFn reducef, Object init) {
      Object ret = init;
      for (int i=0; i<n; ++i) {
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
        int numRecords = recordGroupsMetadata[i].numRecords;
        for (int j=0; j<(numRecords/n); ++j) {
          ret = combinef.invoke(ret, foldedBundle);
        }
        ret = combinef.invoke(ret, reduce(numRecords % n, reducef, init));
      }
      return ret;
    }

  }

  static ISeq repeat(final long n, final Object v) {
    return new LazySeq(new AFn(){
        public ISeq invoke() {
          if (n == 0) {
            return null;
          } else {
            return new Cons(v, repeat(n-1, v));
          }
        }
      });
  }

  public static final class LazyView extends View {

    final Assemble.Fn assembleFn;
    final Schema.Column[] queriedColumns;
    final Reader reader;
    final int defaultBundleSize;
    ISeq assembledBundleseq = null;

    LazyView(Reader reader, Schema.Column[] queriedColumns, Assemble.Fn assembleFn, int defaultBundleSize) {
      this.reader = reader;
      this.queriedColumns = queriedColumns;
      this.assembleFn = assembleFn;
      this.defaultBundleSize = defaultBundleSize;
    }

    static ISeq getBundlesSeq(final ISeq recordGroupReaders, final int bundleSize, final ISeq bundles) {
      return new LazySeq(new AFn() {
          public ISeq invoke() {
            if (RT.seq(bundles) == null) {
              if (RT.seq(recordGroupReaders) == null) {
                return null;
              } else {
                RecordGroup.Reader recordGroupReader = (RecordGroup.Reader)recordGroupReaders.first();
                ISeq newBundles = recordGroupReader.readBundled(bundleSize);
                return new Cons(newBundles.first(),
                                getBundlesSeq(recordGroupReaders.next(), bundleSize, newBundles.next()));
              }
            } else {
              return new Cons(bundles.first(),
                              getBundlesSeq(recordGroupReaders, bundleSize, bundles.next()));
            }
          }
        });
    }

    ISeq getBundlesSeq(int bundleSize) {
      return getBundlesSeq(reader.getRecordGroupReaders(queriedColumns), bundleSize, null);
    }

    ISeq getAssembledBundleSeq(int bundleSize) {
      IFn fn = new AFn() {
          public IChunkedSeq invoke(Object bundle) {
            return ((Bundle)bundle).assemble(assembleFn);
          }
        };
      return Utils.pmap(fn, getBundlesSeq(bundleSize));
    }

    @Override
    public synchronized ISeq seq() {
      if (assembledBundleseq == null) {
        assembledBundleseq = getAssembledBundleSeq(defaultBundleSize);
      }
      return Utils.flattenChunked(assembledBundleseq);
    }

    @Override
    public synchronized Object fold(final int n, final IFn combinef, final IFn reducef) {
      final Object init = combinef.invoke();
      if (assembledBundleseq == null) {
        IFn fn = new AFn() {
            public Object invoke(Object bundle) {
              return ((Bundle)bundle).reduce(reducef, assembleFn, init);
            }
          };
        return reduce(combinef, init, Utils.pmap(fn, getBundlesSeq(n)));
      } else {
        IFn fn = new AFn() {
            public Object invoke(Object assembledBundle) {
              return reduce(reducef, init, assembledBundle);
            }
          };
        return reduce(reducef, init, Utils.pmap(fn, assembledBundleseq));
      }
    }
  }

  static Object reduce(IFn fn, Object init, Object coll) {
    Object ret = init;
    for (ISeq s = RT.seq(coll); s != null; s = s.next()) {
      ret = fn.invoke(ret, s.first());
    }
    return ret;
  }
}
