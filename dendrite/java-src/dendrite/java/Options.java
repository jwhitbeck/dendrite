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

import clojure.lang.Keyword;
import clojure.lang.IFn;
import clojure.lang.IPersistentMap;
import clojure.lang.IMapEntry;
import clojure.lang.ISeq;
import clojure.lang.ITransientCollection;
import clojure.lang.PersistentArrayMap;
import clojure.lang.RT;
import clojure.lang.Symbol;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public final class Options {

  public static final Keyword
    RECORD_GROUP_LENGTH = Keyword.intern("record-group-length"),
    DATA_PAGE_LENGTH = Keyword.intern("data-page-length"),
    OPTIMIZE_COLUMNS = Keyword.intern("optimize-columns?"),
    COMPRESSION_THRESHOLDS = Keyword.intern("compression-thresholds"),
    INVALID_INPUT_HANDLER = Keyword.intern("invalid-input-handler"),
    CUSTOM_TYPES = Keyword.intern("custom-types"),
    QUERY = Keyword.intern("query"),
    ENTRYPOINT = Keyword.intern("entrypoint"),
    MISSING_FIELDS_AS_NIL = Keyword.intern("missing-fields-as-nil?"),
    READERS = Keyword.intern("readers"),
    MAP_FN = Keyword.intern("map-fn"),
    SAMPLE_FN = Keyword.intern("sample-fn"),
    FILTER_FN = Keyword.intern("filter-fn"),
    ALL = Keyword.intern("all"),
    NONE = Keyword.intern("none"),
    DEFAULT = Keyword.intern("default"),
    BASE_TYPE = Keyword.intern("base-type"),
    COERCION_FN = Keyword.intern("coercion-fn"),
    TO_BASE_TYPE_FN = Keyword.intern("to-base-type-fn"),
    FROM_BASE_TYPE_FN = Keyword.intern("from-base-type-fn");

  public static final int DEFAULT_RECORD_GROUP_LENGTH = 128 * 1024 * 1024; // 128 MB
  public static final int DEFAULT_DATA_PAGE_LENGTH = 256 * 1024; // 256 KB
  public static final int DEFAULT_OPTIMIZE_COLUMNS = RecordGroup.ONLY_DEFAULT;
  public static final Map<Symbol,Double> DEFAULT_COMPRESSION_THRESHOLDS;
  public static final boolean DEFAULT_MISSING_FIELDS_AS_NIL = true;
  public static final int DEFAULT_BUNDLE_SIZE = 256;

  static {
    DEFAULT_COMPRESSION_THRESHOLDS = new HashMap<Symbol, Double>();
    DEFAULT_COMPRESSION_THRESHOLDS.put(Types.DEFLATE_SYM, 1.5);
  }

  public static final class CustomTypeDefinition {
    public final Symbol typeSymbol;
    public final Symbol baseTypeSymbol;
    public final IFn coercionFn;
    public final IFn toBaseTypeFn;
    public final IFn fromBaseTypeFn;

    CustomTypeDefinition(Symbol typeSymbol, Symbol baseTypeSymbol, IFn coercionFn, IFn toBaseTypeFn,
                         IFn fromBaseTypeFn) {
      this.typeSymbol = typeSymbol;
      this.baseTypeSymbol = baseTypeSymbol;
      this.coercionFn = coercionFn;
      this.toBaseTypeFn = toBaseTypeFn;
      this.fromBaseTypeFn = fromBaseTypeFn;
    }
  }

  private static Keyword[] validCustomTypeDefinitionKeys
    = new Keyword[]{BASE_TYPE, COERCION_FN, TO_BASE_TYPE_FN, FROM_BASE_TYPE_FN};

  public static CustomTypeDefinition getCustomTypeDefinition(Symbol customTypeSymbol,
                                                             IPersistentMap customTypeDefinitionMap) {
    try {
      checkValidKeys(customTypeDefinitionMap, validCustomTypeDefinitionKeys,
                     "%s is not a valid custom type definition key");
      return new CustomTypeDefinition(customTypeSymbol,
                                      getBaseTypeSymbol(customTypeDefinitionMap),
                                      getLogicalTypeFn(customTypeDefinitionMap, COERCION_FN),
                                      getLogicalTypeFn(customTypeDefinitionMap, TO_BASE_TYPE_FN),
                                      getLogicalTypeFn(customTypeDefinitionMap, FROM_BASE_TYPE_FN));
    } catch (Exception e) {
      throw new IllegalArgumentException(String.format("Error parsing custom-type '%s'.", customTypeSymbol),
                                         e);
    }
  }

  private static Symbol getBaseTypeSymbol(IPersistentMap customTypeDefinitionMap) {
    Object o = RT.get(customTypeDefinitionMap, BASE_TYPE, notFound);
    if (o == notFound) {
      throw new IllegalArgumentException("Required field :base-type is missing.");
    }
    if (!(o instanceof Symbol)) {
      throw new IllegalArgumentException(String.format("Base type '%s' is not a symbol.", o));
    }
    return (Symbol)o;
  }

  private static IFn getLogicalTypeFn(IPersistentMap customTypeDefinitionMap, Keyword kw) {
    Object o = RT.get(customTypeDefinitionMap, kw, notFound);
    if (o == notFound) {
      return null;
    }
    if (!(o instanceof IFn)) {
      throw new IllegalArgumentException(String.format("%s expects a function.", kw));
    }
    return (IFn)o;
  }

  public static List<CustomTypeDefinition> getCustomTypeDefinitions(IPersistentMap options) {
    Object o = RT.get(options, CUSTOM_TYPES, notFound);
    if (o == notFound) {
      return Collections.emptyList();
    } else if (o instanceof IPersistentMap) {
      List<CustomTypeDefinition> customTypeDefinitions = new ArrayList<CustomTypeDefinition>();
      for (ISeq s = RT.seq(o); s != null; s = s.next()) {
        IMapEntry e = (IMapEntry)s.first();
        Object key = e.key();
        Object val = e.val();
        if (!(key instanceof Symbol)) {
          throw new IllegalArgumentException(String.format("custom type key shoud be a symbol but got '%s'",
                                                           key));
        } else if (!(val instanceof IPersistentMap)) {
          throw new IllegalArgumentException(String.format("custom type value should be a map but got '%s'",
                                                           val));
        } else {
          customTypeDefinitions.add(getCustomTypeDefinition((Symbol)key, (IPersistentMap)val));
        }
      }
      return customTypeDefinitions;
    } else {
      throw new IllegalArgumentException(String.format("%s expects a map but got '%s'",
                                                       CUSTOM_TYPES, o));
    }
  }

  public static final class ReaderOptions {
    public final List<CustomTypeDefinition> customTypeDefinitions;
    public ReaderOptions(List<CustomTypeDefinition> customTypeDefinitions) {
      this.customTypeDefinitions = customTypeDefinitions;
    }
  }

  static void checkValidKeys(IPersistentMap options, Keyword[] validKeys, String invalidFormatString) {
    for (ISeq s = RT.seq(options); s != null; s = s.next()) {
      Object key = ((IMapEntry)s.first()).key();
      boolean valid = false;
      for (int i=0; i<validKeys.length; ++i) {
        if (key == validKeys[i] ){
          valid = true;
          break;
        }
      }
      if (!valid) {
        throw new IllegalArgumentException(String.format(invalidFormatString, key));
      }
    }
  }

  static Keyword[] validReaderOptionKeys = new Keyword[]{CUSTOM_TYPES};

  public static ReaderOptions getReaderOptions(IPersistentMap options) {
    checkValidKeys(options, validReaderOptionKeys, "%s is not a supported reader option.");
    return new ReaderOptions(getCustomTypeDefinitions(options));
  }

  public static final class ReadOptions {
    public final Object query;
    public final List<Keyword> entrypoint;
    public final boolean isMissingFieldsAsNil;
    public final Map<Symbol,IFn> readers;
    public final int bundleSize = DEFAULT_BUNDLE_SIZE;
    public final IFn mapFn;
    public final IFn sampleFn;
    public final IFn filterFn;

    public ReadOptions(Object query, List<Keyword> entrypoint, boolean isMissingFieldsAsNil,
                       Map<Symbol,IFn> readers, IFn mapFn, IFn sampleFn, IFn filterFn) {
      this.query = query;
      this.entrypoint = entrypoint;
      this.isMissingFieldsAsNil = isMissingFieldsAsNil;
      this.readers = readers;
      this.mapFn = mapFn;
      this.sampleFn = sampleFn;
      this.filterFn = filterFn;
    }

    public ReadOptions withMapFn(IFn aMapFn) {
      final IFn f = (mapFn == null)? aMapFn : Utils.comp(aMapFn, mapFn);
      return new ReadOptions(this.query, this.entrypoint, this.isMissingFieldsAsNil,
                             this.readers, f, sampleFn, filterFn);
    }


    public ReadOptions withSampleFn(IFn aSampleFn) {
      if (sampleFn != null) {
        throw new IllegalArgumentException("Cannot define multiple sample functions.");
      }
      return new ReadOptions(this.query, this.entrypoint, this.isMissingFieldsAsNil,
                             this.readers, mapFn, aSampleFn, filterFn);
    }

    public ReadOptions withFilterFn(IFn aFilterFn) {
      final IFn f = (filterFn == null)? aFilterFn : Utils.and(filterFn, aFilterFn);
      return new ReadOptions(this.query, this.entrypoint, this.isMissingFieldsAsNil,
                             this.readers, mapFn, sampleFn, f);
    }

  }

  static Keyword[] validReadOptionKeys
    = new Keyword[]{QUERY, ENTRYPOINT, MISSING_FIELDS_AS_NIL, READERS, MAP_FN};

  static Object getQuery(IPersistentMap options) {
    return RT.get(options, QUERY, Schema.SUB_SCHEMA);
  }

  static List<Keyword> getEntrypoint(IPersistentMap options) {
    Object o = RT.get(options, ENTRYPOINT);
    ISeq s;
    try {
      s = RT.seq(o);
    } catch (Exception e) {
      throw new IllegalArgumentException(String.format("%s expects a seqable object but got '%s'",
                                                       ENTRYPOINT, o));
    }
    List<Keyword> entrypoint = new ArrayList<Keyword>();
    for (; s != null; s = s.next()) {
      Object k = s.first();
      if (!(k instanceof Keyword)) {
        throw new IllegalArgumentException(String.format("entrypoint can only contain keywords, but got '%s'",
                                                         k));
      } else {
        entrypoint.add((Keyword)k);
      }
    }
    return entrypoint;
  }

  static boolean getMissingFieldsAsNil(IPersistentMap options) {
    Object o = RT.get(options, MISSING_FIELDS_AS_NIL, notFound);
    if (o == notFound) {
      return DEFAULT_MISSING_FIELDS_AS_NIL;
    } else if (o instanceof Boolean) {
      return (Boolean)o;
    } else {
      throw new IllegalArgumentException(String.format("%s expects a boolean but got '%s'",
                                                       MISSING_FIELDS_AS_NIL, o));
    }
  }

  static void checkTagReader(Object k, Object v) {
    if (!(k instanceof Symbol)) {
      throw new IllegalArgumentException(String.format("reader key should be a symbol but got '%s'.", k));
    }
    if (!(v instanceof IFn)) {
      throw new IllegalArgumentException(
          String.format("reader value for tag '%s' should be a function but got '%s'", k, v));
    }
  }

  static Map<Symbol,IFn> getTagReaders(IPersistentMap options) {
    Object o = RT.get(options, READERS);
    if (o == null) {
      return null;
    } else if (!(o instanceof IPersistentMap)) {
      throw new IllegalArgumentException(String.format("%s expects a map but got '%s'", READERS, o));
    }
    Map<Symbol,IFn> readers = new HashMap<Symbol,IFn>();
    for (Object obj : (IPersistentMap)o) {
      IMapEntry e = (IMapEntry)obj;
      checkTagReader(e.key(), e.val());
      readers.put((Symbol)e.key(), (IFn)e.val());
    }
    return readers;
  }

  static IFn getFn(Keyword key, IPersistentMap options) {
    Object o = RT.get(options, key, notFound);
    if (o == notFound) {
      return null;
    } else if (!(o instanceof IFn)) {
      throw new IllegalArgumentException(String.format("%s expects a function but got '%s'", key, o));
    } else {
      return (IFn)o;
    }
  }

  public static ReadOptions getReadOptions(IPersistentMap options) {
    checkValidKeys(options, validReadOptionKeys, "%s is not a supported read option.");
    return new ReadOptions(getQuery(options),
                           getEntrypoint(options),
                           getMissingFieldsAsNil(options),
                           getTagReaders(options),
                           getFn(MAP_FN, options),
                           getFn(SAMPLE_FN, options),
                           getFn(FILTER_FN, options));
  }

  public static final class WriterOptions {
    public final int recordGroupLength;
    public final int dataPageLength;
    public final int optimizationStrategy;
    public final Map<Symbol,Double> compressionThresholds;
    public final IFn invalidInputHandler;
    public final List<CustomTypeDefinition> customTypeDefinitions;
    public final int bundleSize = DEFAULT_BUNDLE_SIZE;
    public final IFn mapFn;

    public WriterOptions(int recordGroupLength, int dataPageLength, int optimizationStrategy,
                         Map<Symbol,Double> compressionThresholds, IFn invalidInputHandler,
                         List<CustomTypeDefinition> customTypeDefinitions, IFn mapFn) {
      this.recordGroupLength = recordGroupLength;
      this.dataPageLength = dataPageLength;
      this.optimizationStrategy = optimizationStrategy;
      this.compressionThresholds = compressionThresholds;
      this.invalidInputHandler = invalidInputHandler;
      this.customTypeDefinitions = customTypeDefinitions;
      this.mapFn = mapFn;
    }
  }

  final static Keyword[] validWriterOptionKeys
    = new Keyword[]{RECORD_GROUP_LENGTH, DATA_PAGE_LENGTH, OPTIMIZE_COLUMNS, COMPRESSION_THRESHOLDS,
                    INVALID_INPUT_HANDLER, CUSTOM_TYPES, MAP_FN};

  final static Object notFound = new Object();

  static int getPositiveInt(IPersistentMap options, Keyword key, int defaultValue) {
    Object o = RT.get(options, key, notFound);
    if (o == notFound) {
      return defaultValue;
    } else {
      int v;
      try {
        v = RT.intCast(o);
      } catch (Exception e) {
        throw new IllegalArgumentException(String.format("%s expects a positive int but got '%s'", key, o));
      }
      if (v < 0) {
        throw new IllegalArgumentException(String.format("%s expects a positive int but got '%s'", key, o));
      }
      return v;
    }
  }

  static int getRecordGroupLength(IPersistentMap options) {
    return getPositiveInt(options, RECORD_GROUP_LENGTH, DEFAULT_RECORD_GROUP_LENGTH);
  }

  static int getDataPageLength(IPersistentMap options) {
    return getPositiveInt(options, DATA_PAGE_LENGTH, DEFAULT_DATA_PAGE_LENGTH);
  }

  static int getOptimizationStrategy(IPersistentMap options) {
    Object o = RT.get(options, OPTIMIZE_COLUMNS, notFound);
    if (o == notFound) {
      return DEFAULT_OPTIMIZE_COLUMNS;
    } else if (o instanceof Keyword) {
      Keyword k = (Keyword)o;
      if (k == DEFAULT) {
        return RecordGroup.ONLY_DEFAULT;
      } else if (k == NONE) {
        return RecordGroup.NONE;
      } else if (k == ALL) {
        return RecordGroup.ALL;
      }
    }
    throw new IllegalArgumentException(String.format("%s expects one of %s, %s, or %s but got '%s'",
                                                     OPTIMIZE_COLUMNS, ALL, NONE, DEFAULT, o));
  }

  static Map<Symbol,Double> getCompressionThresholds(IPersistentMap options) {
    Object o = RT.get(options, COMPRESSION_THRESHOLDS);
    if (o == null) {
      return DEFAULT_COMPRESSION_THRESHOLDS;
    } else if (!(o instanceof IPersistentMap)) {
      throw new IllegalArgumentException(String.format("%s expects a map but got '%s'",
                                                       COMPRESSION_THRESHOLDS, o));
    }
    Map<Symbol,Double> compressionThresholds = new HashMap<Symbol,Double>();
    for (Object obj : (IPersistentMap)o) {
      IMapEntry entry = (IMapEntry)obj;
      Object key = entry.key();
      Object val = entry.val();
      if (!(key instanceof Symbol)) {
        throw new IllegalArgumentException(String.format("%s expects its keys to be symbols but got '%s'",
                                                         COMPRESSION_THRESHOLDS, key));
      }
      double threshold;
      try {
        threshold = RT.doubleCast(val);
      } catch (Exception e) {
         throw new IllegalArgumentException(String.format("%s expects its values to be doubles but got '%s'",
                                                          COMPRESSION_THRESHOLDS, val));
      }
      if (threshold < 0) {
        throw new IllegalArgumentException(String.format("%s expects its values to be positive but got %f",
                                                         COMPRESSION_THRESHOLDS, threshold));
      }
      compressionThresholds.put((Symbol)key, threshold);
    }
    return compressionThresholds;
  }

  static IFn getInvalidInputHandler(IPersistentMap options) {
    Object o = RT.get(options, INVALID_INPUT_HANDLER, notFound);
    if (o == notFound) {
      return null;
    } else if (o instanceof IFn) {
      return (IFn)o;
    } else {
      throw new IllegalArgumentException(String.format("%s expects a function but got '%s'",
                                                       INVALID_INPUT_HANDLER, o));
    }
  }

  public static WriterOptions getWriterOptions(IPersistentMap options) {
    checkValidKeys(options, validWriterOptionKeys, "%s is not a supported writer option.");
    return new WriterOptions(getRecordGroupLength(options),
                             getDataPageLength(options),
                             getOptimizationStrategy(options),
                             getCompressionThresholds(options),
                             getInvalidInputHandler(options),
                             getCustomTypeDefinitions(options),
                             getFn(MAP_FN, options));
  }
}
