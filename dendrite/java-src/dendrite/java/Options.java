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
    PMAP_FN = Keyword.intern("pmap-fn"),
    ALL = Keyword.intern("all"),
    NONE = Keyword.intern("none"),
    DEFAULT = Keyword.intern("default");

  public static final int DEFAULT_RECORD_GROUP_LENGTH = 128 * 1024 * 1024; // 128 MB
  public static final int DEFAULT_DATA_PAGE_LENGTH = 256 * 1024; // 256 KB
  public static final int DEFAULT_OPTIMIZE_COLUMNS = RecordGroup.ONLY_DEFAULT;
  public static final IPersistentMap DEFAULT_COMPRESSION_THRESHOLDS
    = new PersistentArrayMap(new Object[]{Types.DEFLATE_SYM, 1.5});
  public static final boolean DEFAULT_MISSING_FIELDS_AS_NIL = true;
  public static final int DEFAULT_BUNDLE_SIZE = 256;

  public static final class ReaderOptions {
    public final IPersistentMap customTypeDefinitions;
    public ReaderOptions(IPersistentMap customTypeDefinitions) {
      this.customTypeDefinitions = customTypeDefinitions;
    }
  }

  static IPersistentMap getCustomTypeDefinitions(IPersistentMap options) {
    Object o = RT.get(options, CUSTOM_TYPES);
    if (o == null) {
      return null;
    } else if (o instanceof IPersistentMap) {
      return (IPersistentMap)o;
    } else {
      throw new IllegalArgumentException(String.format("%s expects a map but got '%s'",
                                                       CUSTOM_TYPES, o));
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

  public final static class ReadOptions {
    public final Object query;
    public final ISeq entrypoint;
    public final boolean isMissingFieldsAsNil;
    public final IPersistentMap readers;
    public final int bundleSize = DEFAULT_BUNDLE_SIZE;

    public ReadOptions(Object query, ISeq entrypoint, boolean isMissingFieldsAsNil, IPersistentMap readers) {
      this.query = query;
      this.entrypoint = entrypoint;
      this.isMissingFieldsAsNil = isMissingFieldsAsNil;
      this.readers = readers;
    }
  }

  static Keyword[] validReadOptionKeys
    = new Keyword[]{QUERY, ENTRYPOINT, MISSING_FIELDS_AS_NIL, READERS, PMAP_FN};

  static Object getQuery(IPersistentMap options) {
    return RT.get(options, QUERY, Schema.SUB_SCHEMA);
  }

  static ISeq getEntrypoint(IPersistentMap options) {
    Object o = RT.get(options, ENTRYPOINT);
    try {
      return RT.seq(o);
    } catch (Exception e) {
      throw new IllegalArgumentException(String.format("%s expects a seqable object but got '%s'",
                                                       ENTRYPOINT, o));
    }
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
      throw new IllegalArgumentException
        (String.format("reader value for tag '%s' should be a function but got '%s'", k, v));
    }
  }

  static IPersistentMap getTagReaders(IPersistentMap options) {
    Object o = RT.get(options, READERS);
    if (o == null) {
      return null;
    } else if (!(o instanceof IPersistentMap)) {
      throw new IllegalArgumentException(String.format("%s expects a map but got '%s'", READERS, o));
    }
    IPersistentMap readers = (IPersistentMap)o;
    for (Object obj : readers) {
      IMapEntry e = (IMapEntry)obj;
      checkTagReader(e.key(), e.val());
    }
    return readers;
  }

  public static ReadOptions getReadOptions(IPersistentMap options) {
    checkValidKeys(options, validReadOptionKeys, "%s is not a supported read option.");
    return new ReadOptions(getQuery(options),
                           getEntrypoint(options),
                           getMissingFieldsAsNil(options),
                           getTagReaders(options));

  }

  public final static class WriterOptions {
    public final int recordGroupLength;
    public final int dataPageLength;
    public final int optimizationStrategy;
    public final IPersistentMap compressionThresholds;
    public final IFn invalidInputHandler;
    public final IPersistentMap customTypeDefinitions;
    public final int bundleSize = DEFAULT_BUNDLE_SIZE;

    public WriterOptions(int recordGroupLength, int dataPageLength, int optimizationStrategy,
                         IPersistentMap compressionThresholds, IFn invalidInputHandler,
                         IPersistentMap customTypeDefinitions) {
      this.recordGroupLength = recordGroupLength;
      this.dataPageLength = dataPageLength;
      this.optimizationStrategy = optimizationStrategy;
      this.compressionThresholds = compressionThresholds;
      this.invalidInputHandler = invalidInputHandler;
      this.customTypeDefinitions = customTypeDefinitions;
    }
  }

  final static Keyword[] validWriterOptionKeys
    = new Keyword[]{RECORD_GROUP_LENGTH, DATA_PAGE_LENGTH, OPTIMIZE_COLUMNS, COMPRESSION_THRESHOLDS,
                    INVALID_INPUT_HANDLER, CUSTOM_TYPES};

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

  static IPersistentMap getCompressionThresholds(IPersistentMap options) {
    Object o = RT.get(options, COMPRESSION_THRESHOLDS);
    if (o == null) {
      return DEFAULT_COMPRESSION_THRESHOLDS;
    } else if (!(o instanceof IPersistentMap)) {
      throw new IllegalArgumentException(String.format("%s expects a map but got '%s'",
                                                       COMPRESSION_THRESHOLDS, o));
    }
    IPersistentMap compressionThresholds = (IPersistentMap)o;
    for (Object obj : compressionThresholds) {
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
                             getCustomTypeDefinitions(options));
  }
}
