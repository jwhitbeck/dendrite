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
import clojure.lang.IChunk;
import clojure.lang.IFn;
import clojure.lang.IPersistentMap;
import clojure.lang.IPersistentVector;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;
import clojure.lang.Symbol;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

public final class FileReader implements Closeable, IReader {

  public static final Keyword
    RECORD_GROUPS = Keyword.intern("record-groups"),
    COLUMNS = Keyword.intern("columns"),
    GLOBAL = Keyword.intern("global"),
    DATA_HEADER_LENGTH = Keyword.intern("data-header-length"),
    REPETITION_LEVELS_LENGTH = Keyword.intern("repetition-levels-length"),
    DEFINITION_LEVELS_LENGTH = Keyword.intern("definition-levels-length"),
    METADATA_LENGTH = Keyword.intern("metadata-length"),
    DATA_LENGTH = Keyword.intern("data-length"),
    DICTIONARY_HEADER_LENGTH = Keyword.intern("dictionary-header-length"),
    DICTIONARY_LENGTH = Keyword.intern("dictionary-length"),
    NUM_VALUES = Keyword.intern("num-values"),
    NUM_NON_NIL_VALUES = Keyword.intern("num-non-nil-values"),
    LENGTH = Keyword.intern("length"),
    NUM_PAGES = Keyword.intern("num-pages"),
    NUM_DICTIONARY_VALUES = Keyword.intern("num-dictionary-values"),
    TYPE = Keyword.intern("type"),
    ENCODING = Keyword.intern("encoding"),
    COMPRESSION = Keyword.intern("compression"),
    MAX_REPETITION_LEVEL = Keyword.intern("max-repetition-level"),
    MAX_DEFINITION_LEVEL = Keyword.intern("max-definition-level"),
    PATH = Keyword.intern("path"),
    NUM_RECORDS = Keyword.intern("num-records"),
    NUM_RECORD_GROUPS = Keyword.intern("num-record-groups"),
    NUM_COLUMN_CHUNKS = Keyword.intern("num-column-chunks"),
    NUM_COLUMNS = Keyword.intern("num-columns");

  final Types types;
  final FileChannel fileChannel;
  final Metadata.File fileMetadata;
  final long metadataLength;

  private FileReader(Types types, FileChannel fileChannel, Metadata.File fileMetadata, long metadataLength) {
    this.types = types;
    this.fileChannel = fileChannel;
    this.fileMetadata = fileMetadata;
    this.metadataLength = metadataLength;
  }

  public static FileReader create(Options.ReaderOptions options, File file) throws IOException {
    FileChannel fileChannel = Utils.getReadingFileChannel(file);
    MetadataReadResult res = readMetadata(fileChannel);
    Types types = Types.create(options.customTypeDefinitions, res.fileMetadata.customTypes);
    return new FileReader(types, fileChannel, res.fileMetadata, res.metadataLength);
  }

  public ByteBuffer getMetadata() {
    return fileMetadata.metadata;
  }

  public long getNumRecords() {
    return fileMetadata.getNumRecords();
  }

  public Map<Symbol, Symbol> getCustomTypeMappings() {
    Map<Symbol, Symbol> customTypeMappings = new HashMap<Symbol, Symbol>();
    for (CustomType ct : fileMetadata.customTypes) {
      customTypeMappings.put(ct.sym, types.getTypeSymbol(ct.baseType));
    }
    return customTypeMappings;
  }

  Schema.QueryResult getQueryResult(Options.ReadOptions options) {
    Schema.QueryResult res = Schema.applyQuery(types,
                                               options.isMissingFieldsAsNil,
                                               options.readers,
                                               Schema.getSubSchema(options.entrypoint, fileMetadata.schema),
                                               options.query);
    if (!options.mapFns.isEmpty()) {
      IFn fn = res.schema.fn;
      if (fn == null) {
        fn = Utils.comp(options.mapFns);
      } else {
        fn = Utils.comp(Utils.copyAndAddFirst(fn, options.mapFns));
      }
      return new Schema.QueryResult(res.schema.withFn(fn), res.columns);
    } else {
      return res;
    }
  }

  @Override
  public View read(Options.ReadOptions options) {
    return new LazyView(options);
  }

  private Iterator<RecordGroup.Reader> getRecordGroupReaders(Schema.Column[] columns, int bundleSize) {
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

  private IPersistentMap asPersistentMap(Stats.Global globalStats) {
    return PersistentArrayMap.EMPTY.asTransient()
      .assoc(NUM_COLUMNS, globalStats.numColumns)
      .assoc(NUM_RECORD_GROUPS, globalStats.numRecordGroups)
      .assoc(NUM_RECORDS, globalStats.numRecords)
      .assoc(LENGTH, globalStats.length)
      .assoc(DATA_HEADER_LENGTH, globalStats.dataHeaderLength)
      .assoc(REPETITION_LEVELS_LENGTH, globalStats.repetitionLevelsLength)
      .assoc(DEFINITION_LEVELS_LENGTH, globalStats.definitionLevelsLength)
      .assoc(DATA_LENGTH, globalStats.dataLength)
      .assoc(DICTIONARY_HEADER_LENGTH, globalStats.dictionaryHeaderLength)
      .assoc(DICTIONARY_LENGTH, globalStats.dictionaryLength)
      .assoc(METADATA_LENGTH, globalStats.metadataLength)
      .persistent();
  }

  private IPersistentMap asPersistentMap(Stats.Column columnStats) {
    return PersistentArrayMap.EMPTY.asTransient()
      .assoc(TYPE, columnStats.type)
      .assoc(ENCODING, columnStats.encoding)
      .assoc(COMPRESSION, columnStats.compression)
      .assoc(PATH, columnStats.path)
      .assoc(MAX_REPETITION_LEVEL, columnStats.maxRepetitionLevel)
      .assoc(MAX_DEFINITION_LEVEL, columnStats.maxDefinitionLevel)
      .assoc(NUM_COLUMN_CHUNKS, columnStats.numColumnChunks)
      .assoc(NUM_VALUES, columnStats.numValues)
      .assoc(NUM_NON_NIL_VALUES, columnStats.numNonNilValues)
      .assoc(LENGTH, columnStats.length)
      .assoc(DATA_HEADER_LENGTH, columnStats.dataHeaderLength)
      .assoc(REPETITION_LEVELS_LENGTH, columnStats.repetitionLevelsLength)
      .assoc(DEFINITION_LEVELS_LENGTH, columnStats.definitionLevelsLength)
      .assoc(DATA_LENGTH, columnStats.dataLength)
      .assoc(NUM_DICTIONARY_VALUES, columnStats.numDictionaryValues)
      .assoc(DICTIONARY_HEADER_LENGTH, columnStats.dictionaryHeaderLength)
      .assoc(DICTIONARY_LENGTH, columnStats.dictionaryLength)
      .persistent();
  }

  private IPersistentMap asPersistentMap(Stats.RecordGroup recordGroupStats) {
    return PersistentArrayMap.EMPTY.asTransient()
      .assoc(NUM_RECORDS, recordGroupStats.numRecords)
      .assoc(NUM_COLUMN_CHUNKS, recordGroupStats.numColumnChunks)
      .assoc(LENGTH, recordGroupStats.length)
      .assoc(DATA_HEADER_LENGTH, recordGroupStats.dataHeaderLength)
      .assoc(REPETITION_LEVELS_LENGTH, recordGroupStats.repetitionLevelsLength)
      .assoc(DEFINITION_LEVELS_LENGTH, recordGroupStats.definitionLevelsLength)
      .assoc(DATA_LENGTH, recordGroupStats.dataLength)
      .assoc(DICTIONARY_HEADER_LENGTH, recordGroupStats.dictionaryHeaderLength)
      .assoc(DICTIONARY_LENGTH, recordGroupStats.dictionaryLength)
      .persistent();
  }

  public IPersistentMap getStats() throws IOException {
    IPersistentVector[] paths = Schema.getPaths(fileMetadata.schema);
    Schema.Column[] columns = Schema.getColumns(fileMetadata.schema);
    List<Stats.RecordGroup> recordGroupsStats = new ArrayList<Stats.RecordGroup>();
    List<List<Stats.ColumnChunk>> columnChunkStatsByColumn
      = new ArrayList<List<Stats.ColumnChunk>>(columns.length);
    for (int i=0; i<columns.length; ++i) {
      columnChunkStatsByColumn.add(new ArrayList<Stats.ColumnChunk>());
    }
    Iterator<RecordGroup.Reader> recordGroupReaders = getRecordGroupReaders(columns, 100);
    while (recordGroupReaders.hasNext()) {
      RecordGroup.Reader recordGroupReader = recordGroupReaders.next();
      List<Stats.ColumnChunk> columnChunksStats = recordGroupReader.getColumnChunkStats();
      recordGroupsStats.add(Stats.createRecordGroupStats(recordGroupReader.getNumRecords(),
                                                         columnChunksStats));
      int i = 0;
      for(Stats.ColumnChunk columnChunkStats : columnChunksStats) {
        columnChunkStatsByColumn.get(i).add(columnChunkStats);
        i += 1;
      }
    }
    List<Stats.Column> columnsStats = new ArrayList<Stats.Column>(columns.length);
    for (int i=0; i<columns.length; ++i) {
      Schema.Column col = columns[i];
      columnsStats.add(Stats.createColumnStats(types.getTypeSymbol(col.type),
                                               types.getEncodingSymbol(col.encoding),
                                               types.getCompressionSymbol(col.compression),
                                               col.repetitionLevel,
                                               col.definitionLevel,
                                               paths[i],
                                               columnChunkStatsByColumn.get(i)));
    }
    List<IPersistentMap> recordGroupStatsMaps = new ArrayList<IPersistentMap>();
    for (Stats.RecordGroup recordGroupStats : recordGroupsStats) {
      recordGroupStatsMaps.add(asPersistentMap(recordGroupStats));
    }
    List<IPersistentMap> columnStatsMaps = new ArrayList<IPersistentMap>();
    for (Stats.Column columnStats : columnsStats) {
      columnStatsMaps.add(asPersistentMap(columnStats));
    }
    IPersistentMap globalStatsMap = asPersistentMap(Stats.createGlobalStats(fileChannel.size(),
                                                                            metadataLength,
                                                                            columns.length,
                                                                            recordGroupsStats));
    return new PersistentArrayMap(new Object[]{
        RECORD_GROUPS, recordGroupStatsMaps,
        COLUMNS, columnStatsMaps,
        GLOBAL, globalStatsMap
      });
  }

  public Object getSchema() {
    return Schema.unparse(types, fileMetadata.schema);
  }

  public Object getPlainSchema() {
    return Schema.unparsePlain(types, fileMetadata.schema);
  }

  @Override
  public void close() throws IOException {
    fileChannel.close();
  }

  private static boolean isValidMagicBytes(ByteBuffer bb) {
    return Arrays.equals(Constants.magicBytes, Types.toByteArray(bb));
  }

  private static final int fixedIntLength = 4;

  private static final class MetadataReadResult {
    final Metadata.File fileMetadata;
    final long metadataLength;

    MetadataReadResult(Metadata.File fileMetadata, long metadataLength) {
      this.fileMetadata = fileMetadata;
      this.metadataLength = metadataLength;
    }
  }

  private static MetadataReadResult readMetadata(FileChannel fileChannel) throws IOException {
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
    return new MetadataReadResult(Metadata.File.read(metadataBuffer), metadataLength);
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
          return bundleIterator != null && bundleIterator.hasNext();
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

  private interface IAssembleFutureFactory {
    Future<IChunk> get(Bundle bundle);
  }

  private static final class AssembleFutureFactory implements IAssembleFutureFactory {
    private final Assemble.Fn assembleFn;

    AssembleFutureFactory(Assemble.Fn assembleFn) {
      this.assembleFn = assembleFn;
    }

    @Override
    public Future<IChunk> get(final Bundle bundle) {
      return Agent.soloExecutor.submit(new Callable<IChunk>() {
          public IChunk call() {
            return bundle.assemble(assembleFn);
          }
        });
    }
  }

  private static final class AssembleSampledFutureFactory implements IAssembleFutureFactory {
    private final Assemble.Fn assembleFn;
    private final IFn sampleFn;

    AssembleSampledFutureFactory(Assemble.Fn assembleFn, IFn sampleFn) {
      this.assembleFn = assembleFn;
      this.sampleFn = sampleFn;
    }

    @Override
    public Future<IChunk> get(final Bundle bundle) {
      return Agent.soloExecutor.submit(new Callable<IChunk>() {
          public IChunk call() {
            return bundle.assembleSampled(assembleFn, sampleFn);
          }
        });
    }
  }

  private static final class AssembleMangledFutureFactory implements IAssembleFutureFactory {
    private final Assemble.Fn assembleFn;
    private final Mangle.Fn mangleFn;

    AssembleMangledFutureFactory(Assemble.Fn assembleFn, Mangle.Fn mangleFn) {
      this.assembleFn = assembleFn;
      this.mangleFn = mangleFn;
    }

    @Override
    public Future<IChunk> get(final Bundle bundle) {
      return Agent.soloExecutor.submit(new Callable<IChunk>() {
          public IChunk call() {
            return bundle.assembleMangled(assembleFn, mangleFn);
          }
        });
    }
  }

  private static final class AssembleSampledAndMangledFutureFactory implements IAssembleFutureFactory {
    private final Assemble.Fn assembleFn;
    private final IFn sampleFn;
    private final Mangle.Fn mangleFn;

    AssembleSampledAndMangledFutureFactory(Assemble.Fn assembleFn, IFn sampleFn, Mangle.Fn mangleFn) {
      this.assembleFn = assembleFn;
      this.sampleFn = sampleFn;
      this.mangleFn = mangleFn;
    }

    @Override
    public Future<IChunk> get(final Bundle bundle) {
      return Agent.soloExecutor.submit(new Callable<IChunk>() {
          public IChunk call() {
            return bundle.assembleSampledAndMangled(assembleFn, sampleFn, mangleFn);
          }
        });
    }
  }

  private IAssembleFutureFactory getAssembleFutureFactory(Assemble.Fn assembleFn,
                                                          IFn sampleFn,
                                                          Mangle.Fn mangleFn) {
    if (sampleFn == null) {
      if (mangleFn == null) {
        return new AssembleFutureFactory(assembleFn);
      } else {
        return new AssembleMangledFutureFactory(assembleFn, mangleFn);
      }
    } else {
      if (mangleFn == null) {
        return new AssembleSampledFutureFactory(assembleFn, sampleFn);
      } else {
        return new AssembleSampledAndMangledFutureFactory(assembleFn, sampleFn, mangleFn);
      }
    }
  }

  private static Iterator<IChunk> getRecordChunksIterator(final Iterator<Bundle> bundlesIterator,
                                                          final IAssembleFutureFactory assembleFutureFactory) {
    int n = 2 + Runtime.getRuntime().availableProcessors();
    final LinkedList<Future<IChunk>> futures = new LinkedList<Future<IChunk>>();
    int k = 0;
    while (bundlesIterator.hasNext() && k < n) {
      futures.addLast(assembleFutureFactory.get(bundlesIterator.next()));
    }
    return new AReadOnlyIterator<IChunk>() {
      @Override
        public boolean hasNext() {
        return !futures.isEmpty();
      }

      @Override
        public IChunk next() {
        Future<IChunk> fut = futures.pollFirst();
        IChunk chunk = Utils.tryGetFuture(fut);
        if (bundlesIterator.hasNext()) {
          futures.addLast(assembleFutureFactory.get(bundlesIterator.next()));
        }
        return chunk;
      }
    };
  }

  private interface IReduceFutureFactory {
    Future<Object> get(Bundle bundle);
  }

  private static final class ReduceFutureFactory implements IReduceFutureFactory {
    private final Assemble.Fn assembleFn;
    private final IFn reduceFn;
    private final Object init;

    ReduceFutureFactory(Assemble.Fn assembleFn, IFn reduceFn, Object init) {
      this.assembleFn = assembleFn;
      this.reduceFn = reduceFn;
      this.init = init;
    }

    @Override
    public Future<Object> get(final Bundle bundle) {
      return Agent.soloExecutor.submit(new Callable<Object>() {
        public Object call() {
          return bundle.reduce(reduceFn, assembleFn, init);
        }
      });
    }
  }

  private static final class ReduceSampledFutureFactory implements IReduceFutureFactory {
    private final Assemble.Fn assembleFn;
    private final IFn reduceFn;
    private final Object init;
    private final IFn sampleFn;

    ReduceSampledFutureFactory(Assemble.Fn assembleFn, IFn reduceFn, Object init, IFn sampleFn) {
      this.assembleFn = assembleFn;
      this.reduceFn = reduceFn;
      this.init = init;
      this.sampleFn = sampleFn;
    }

    @Override
    public Future<Object> get(final Bundle bundle) {
      return Agent.soloExecutor.submit(new Callable<Object>() {
        public Object call() {
          return bundle.reduceSampled(reduceFn, assembleFn, sampleFn, init);
        }
      });
    }
  }

  private static final class ReduceMangledFutureFactory implements IReduceFutureFactory {
    private final Assemble.Fn assembleFn;
    private final IFn reduceFn;
    private final Object init;
    private final Mangle.Fn mangleFn;

    ReduceMangledFutureFactory(Assemble.Fn assembleFn, IFn reduceFn, Object init, Mangle.Fn mangleFn) {
      this.assembleFn = assembleFn;
      this.reduceFn = reduceFn;
      this.init = init;
      this.mangleFn = mangleFn;
    }

    @Override
    public Future<Object> get(final Bundle bundle) {
      return Agent.soloExecutor.submit(new Callable<Object>() {
        public Object call() {
          return bundle.reduceMangled(reduceFn, assembleFn, mangleFn, init);
        }
      });
    }
  }

  private static final class ReduceSampledAndMangledFutureFactory implements IReduceFutureFactory {
    private final Assemble.Fn assembleFn;
    private final IFn reduceFn;
    private final Object init;
    private final IFn sampleFn;
    private final Mangle.Fn mangleFn;

    ReduceSampledAndMangledFutureFactory(Assemble.Fn assembleFn, IFn reduceFn, Object init, IFn sampleFn,
                                         Mangle.Fn mangleFn) {
      this.assembleFn = assembleFn;
      this.reduceFn = reduceFn;
      this.init = init;
      this.sampleFn = sampleFn;
      this.mangleFn = mangleFn;
    }

    @Override
    public Future<Object> get(final Bundle bundle) {
      return Agent.soloExecutor.submit(new Callable<Object>() {
        public Object call() {
          return bundle.reduceSampledAndMangled(reduceFn, assembleFn, sampleFn, mangleFn, init);
        }
      });
    }
  }

  private IReduceFutureFactory getReduceFutureFactory(IFn reduceFn, Object init, Assemble.Fn assembleFn,
                                                      IFn sampleFn, Mangle.Fn mangleFn) {
    if (sampleFn == null) {
      if (mangleFn == null) {
        return new ReduceFutureFactory(assembleFn, reduceFn, init);
      } else {
        return new ReduceMangledFutureFactory(assembleFn, reduceFn, init, mangleFn);
      }
    } else {
      if (mangleFn == null) {
        return new ReduceSampledFutureFactory(assembleFn, reduceFn, init, sampleFn);
      } else {
        return new ReduceSampledAndMangledFutureFactory(assembleFn, reduceFn, init, sampleFn, mangleFn);
      }
    }
  }

  private static Iterator<Object> getReducedChunksIterator(final Iterator<Bundle> bundlesIterator,
                                                           final IReduceFutureFactory reduceFutureFactory) {
    int n = 2 + Runtime.getRuntime().availableProcessors();
    final LinkedList<Future<Object>> futures = new LinkedList<Future<Object>>();
    int k = 0;
    while (bundlesIterator.hasNext() && k < n) {
      futures.addLast(reduceFutureFactory.get(bundlesIterator.next()));
    }
    return new AReadOnlyIterator<Object>() {
      @Override
        public boolean hasNext() {
        return !futures.isEmpty();
      }

      @Override
        public Object next() {
        Future<Object> fut = futures.pollFirst();
        Object obj = Utils.tryGetFuture(fut);
        if (bundlesIterator.hasNext()) {
          futures.addLast(reduceFutureFactory.get(bundlesIterator.next()));
        }
        return obj;
      }
    };
  }

  public final class LazyView extends View {

    private Options.ReadOptions options;
    private Schema.QueryResult queryResult;
    private Assemble.Fn assembleFn;
    private Mangle.Fn mangleFn;
    private boolean isMangleFnReady;

    LazyView(Options.ReadOptions options) {
      super(options.bundleSize);
      this.options = options;
      this.isMangleFnReady = false;
    }

    private synchronized Schema.QueryResult getQueryResult() {
      if (queryResult == null) {
        queryResult = FileReader.this.getQueryResult(options);
      }
      return queryResult;
    }

    private synchronized Schema.Column[] getQueriedColumns() {
      return getQueryResult().columns;
    }

    private synchronized Assemble.Fn getAssembleFn() {
      if (assembleFn == null) {
        assembleFn = Assemble.getFn(getQueryResult().schema);
      }
      return assembleFn;
    }

    private synchronized Mangle.Fn getMangleFn() {
      if (!isMangleFnReady) {
        mangleFn = Mangle.compose(options.mangleFns);
        isMangleFnReady = true;
      }
      return mangleFn;
    }

    @Override
    protected View withOptions(Options.ReadOptions options) {
      return new LazyView(options);
    }

    @Override
    protected Options.ReadOptions getReadOptions() {
      return options;
    }

    @Override
    protected Iterable<IChunk> getRecordChunks(final int bundleSize) {
      return new Iterable<IChunk>() {
        @Override
        public Iterator<IChunk> iterator() {
          return FileReader.getRecordChunksIterator(getBundlesIterator(bundleSize),
                                                    getAssembleFutureFactory(getAssembleFn(),
                                                                             options.sampleFn,
                                                                             getMangleFn()));
        }
      };
    }

    @Override
    protected Iterable<Object> getReducedChunkValues(final IFn f, final Object init, final int bundleSize) {
      return new Iterable<Object>() {
        @Override
        public Iterator<Object> iterator() {
          return FileReader.getReducedChunksIterator(getBundlesIterator(bundleSize),
                                                     getReduceFutureFactory(f,
                                                                            init,
                                                                            getAssembleFn(),
                                                                            options.sampleFn,
                                                                            getMangleFn()));
        }
      };
    }

    private Iterator<Bundle> getBundlesIterator(int bundleSize) {
      return FileReader.getBundlesIterator(getRecordGroupReaders(getQueriedColumns(), bundleSize));
    }
  }
}
