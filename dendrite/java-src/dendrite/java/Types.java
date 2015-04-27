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
import clojure.lang.BigInt;
import clojure.lang.IFn;
import clojure.lang.IMapEntry;
import clojure.lang.IPersistentCollection;
import clojure.lang.IPersistentMap;
import clojure.lang.ISeq;
import clojure.lang.Keyword;
import clojure.lang.Numbers;
import clojure.lang.Ratio;
import clojure.lang.RT;
import clojure.lang.Symbol;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.UUID;

public final class Types {

  // Primitive types
  public final static int
    BOOLEAN = -1,
    INT = -2,
    LONG = -3,
    FLOAT = -4,
    DOUBLE = -5,
    BYTE_ARRAY = -6,
    FIXED_LENGTH_BYTE_ARRAY = -7;

  private static boolean isPrimitive(int type) {
    return type < 0;
  }

  private static int i(int type) {
    return - type - 1;
  }

  public final static IFn asBoolean = new AFn() {
      public Object invoke(Object o) {
          return RT.booleanCast(o);
      }
    };

  public final static IFn asInt = new AFn() {
      public Object invoke(Object o) {
          return RT.intCast(o);
      }
    };

  public final static IFn asLong = new AFn() {
       public Object invoke(Object o) {
          return RT.longCast(o);
      }
    };

  public final static IFn asFloat = new AFn() {
       public Object invoke(Object o) {
         return RT.floatCast(o);
      }
    };

  public final static IFn asDouble = new AFn() {
       public Object invoke(Object o) {
         return RT.doubleCast(o);
      }
    };

  public final static IFn asByteArray = new AFn() {
       public Object invoke(Object o) {
         if (o instanceof byte[]) {
           return o;
         }
         return Numbers.byte_array(o);
      }
    };

  private final static Symbol[] primitiveTypeSymbols;
  private final static HashMap<Symbol,Integer> primitiveTypes;
  private final static IFn[] primitiveCoercionFns;

  static {
    primitiveTypeSymbols = new Symbol[-FIXED_LENGTH_BYTE_ARRAY];
    primitiveTypeSymbols[i(BOOLEAN)] = Symbol.intern("boolean");
    primitiveTypeSymbols[i(INT)] = Symbol.intern("int");
    primitiveTypeSymbols[i(LONG)] = Symbol.intern("long");
    primitiveTypeSymbols[i(FLOAT)] = Symbol.intern("float");
    primitiveTypeSymbols[i(DOUBLE)] = Symbol.intern("double");
    primitiveTypeSymbols[i(BYTE_ARRAY)] = Symbol.intern("byte-array");
    primitiveTypeSymbols[i(FIXED_LENGTH_BYTE_ARRAY)] = Symbol.intern("fixed-length-byte-array");

    primitiveTypes = new HashMap<Symbol,Integer>(- 2 * FIXED_LENGTH_BYTE_ARRAY);
    for (int i=0; i<primitiveTypeSymbols.length; ++i) {
      primitiveTypes.put(primitiveTypeSymbols[i], -1-i);
    }

    primitiveCoercionFns = new IFn[-FIXED_LENGTH_BYTE_ARRAY];
    primitiveCoercionFns[i(BOOLEAN)] = asBoolean;
    primitiveCoercionFns[i(INT)] = asInt;
    primitiveCoercionFns[i(LONG)] = asLong;
    primitiveCoercionFns[i(FLOAT)] = asFloat;
    primitiveCoercionFns[i(DOUBLE)] = asDouble;
    primitiveCoercionFns[i(BYTE_ARRAY)] = asByteArray;
    primitiveCoercionFns[i(FIXED_LENGTH_BYTE_ARRAY)] = asByteArray;
  }

  // Built-in logical types
  public final static int
    STRING = 0,
    INST = 1,
    UUID = 2,
    CHAR = 3,
    BIGINT = 4,
    BIGDEC = 5,
    RATIO = 6,
    KEYWORD = 7,
    SYMBOL = 8,
    BYTE_BUFFER = 9;

  public final static int FIRST_CUSTOM_TYPE = 10;

  public final static IFn asString = new AFn() {
      public Object invoke(Object o) {
        if (o == null) {
          return "";
        }
        return o.toString();
      }
    };

  public final static IFn asInst = new AFn() {
      public Object invoke(Object o) {
        if (!(o instanceof Date)) {
          throw new IllegalArgumentException(String.format("%s is not an instance of java.util.Date.", o));
        }
        return o;
      }
    };

  public final static IFn asUUID = new AFn() {
      public Object invoke(Object o) {
        if (!(o instanceof UUID)) {
          throw new IllegalArgumentException(String.format("%s is not an instance of java.util.UUID.", o));
        }
        return o;
      }
    };

  public final static IFn asChar = new AFn() {
      public Object invoke(Object o) {
        return RT.charCast(o);
      }
    };

  // adapted from clojure.core/bigint
  public final static IFn asBigInt = new AFn() {
      public Object invoke(Object o) {
        if (o instanceof BigInt) {
          return o;
        } else if (o instanceof BigInteger) {
          return BigInt.fromBigInteger((BigInteger)o);
        } else if (o instanceof BigDecimal) {
          return asBigInt.invoke(((BigDecimal)o).toBigInteger());
        } else if (o instanceof Double || o instanceof Float) {
          return asBigInt.invoke(BigDecimal.valueOf(RT.doubleCast(o)));
        } else if (o instanceof Ratio) {
          return asBigInt.invoke(((Ratio)o).bigIntegerValue());
        } else if (o instanceof Number) {
          return BigInt.valueOf(RT.longCast(o));
        } else if (o instanceof String) {
          return asBigInt.invoke(new BigInteger((String)o));
        } else if (o instanceof byte[]) {
          return asBigInt.invoke(new BigInteger((byte[])o));
        }
        throw new IllegalArgumentException(String.format("'%s' cannot be coerced to a clojure.lang.BigInt.",
                                                         o));
      }
    };

  // adapted from clojure.core/bigdec
  public final static IFn asBigDecimal = new AFn() {
      public Object invoke(Object o) {
        if (o instanceof BigDecimal) {
          return o;
        } else if (o instanceof Double || o instanceof Float) {
          return BigDecimal.valueOf(RT.doubleCast(o));
        } else if (o instanceof Ratio) {
          Ratio r = (Ratio)o;
          return Numbers.divide(new BigDecimal(r.numerator), new BigDecimal(r.denominator));
        } else if (o instanceof BigInt) {
          return ((BigInt)o).toBigDecimal();
        } else if (o instanceof BigInteger) {
          return new BigDecimal((BigInteger)o);
        } else if (o instanceof Number) {
          return BigDecimal.valueOf(RT.longCast(o));
        } else if (o instanceof String) {
          return new BigDecimal((String)o);
        }
        throw new IllegalArgumentException(String.format("'%s' cannot be coerced to a java.math.BigDecimal.",
                                                         o));
      }
    };

  public final static IFn asRatio = new AFn() {
      public Object invoke(Object o) {
        if (o instanceof Ratio) {
          return o;
        }
        try {
          BigInt bi = (BigInt)asBigInt.invoke(o);
          return new Ratio(bi.toBigInteger(), BigInteger.ONE);
        } catch (Exception e) {
          throw new IllegalArgumentException(String.format("'%s' cannot be coerced to a clojure.lang.Ratio.",
                                                           o));
        }
      }
    };

  public final static IFn asKeyword = new AFn() {
      public Object invoke(Object o) {
        if (o instanceof Keyword) {
          return o;
        } else if (o instanceof Symbol) {
          return Keyword.intern((Symbol)o);
        } else if (o instanceof String) {
          return Keyword.intern((String)o);
        }
        throw new IllegalArgumentException(String.format("'%s' cannot be coerced to a clojure.lang.Keyword.",
                                                         o));
      }
    };

  public final static IFn asSymbol = new AFn() {
      public Object invoke(Object o) {
        if (o instanceof Symbol) {
          return o;
        } else if (o instanceof String) {
          return Symbol.intern((String)o);
        }
        throw new IllegalArgumentException(String.format("'%s' cannot be coerced to a clojure.lang.Symbol.",
                                                         o));
      }
    };

  public final static IFn asByteBuffer = new AFn() {
      public Object invoke(Object o) {
        if (o instanceof ByteBuffer) {
          return o;
        } else if (o instanceof byte[]) {
          return ByteBuffer.wrap((byte[])o);
        }
        throw new IllegalArgumentException(String.format("'%s' cannot be coerced to a java.nio.ByteBuffer.",
                                                         o));
      }
    };

  private final static LogicalType
    stringType = new LogicalType(Symbol.intern("string"),
                                 BYTE_ARRAY,
                                 asString,
                                 new AFn() {
                                   public Object invoke(Object o) {
                                     return toByteArray((String)o);
                                   }
                                 },
                                 new AFn() {
                                   public Object invoke(Object o) {
                                     return Types.toString((byte[])o);
                                   }
                                 }),

    instType = new LogicalType(Symbol.intern("inst"),
                               LONG,
                               asInst,
                               new AFn() {
                                 public Object invoke(Object o) {
                                   return toLong((Date)o);
                                 }
                               },
                               new AFn() {
                                 public Object invoke(Object o) {
                                   return toDate((long)o);
                                 }
                               }),

    uuidType = new LogicalType(Symbol.intern("uuid"),
                               FIXED_LENGTH_BYTE_ARRAY,
                               asUUID,
                               new AFn() {
                                 public Object invoke(Object o) {
                                   return toByteArray((UUID)o);
                                 }
                               },
                               new AFn() {
                                 public Object invoke(Object o) {
                                   return toUUID((byte[])o);
                                 }
                               }),

    charType = new LogicalType(Symbol.intern("char"),
                               INT,
                               asChar,
                               new AFn() {
                                 public Object invoke(Object o) {
                                   return RT.intCast(o);
                                 }
                               },
                               new AFn() {
                                 public Object invoke(Object o) {
                                   return RT.charCast(o);
                                 }
                               }),

    bigIntType =  new LogicalType(Symbol.intern("bigint"),
                                  BYTE_ARRAY,
                                  asBigInt,
                                  new AFn() {
                                    public Object invoke(Object o) {
                                      return toByteArray((BigInt)o);
                                    }
                                  },
                                  new AFn() {
                                    public Object invoke(Object o) {
                                      return toBigInt((byte[])o);
                                    }
                                  }),

    bigDecType = new LogicalType(Symbol.intern("bigdec"),
                                 BYTE_ARRAY,
                                 asBigDecimal,
                                 new AFn() {
                                   public Object invoke(Object o) {
                                     return toByteArray((BigDecimal)o);
                                   }
                                 },
                                 new AFn() {
                                   public Object invoke(Object o) {
                                     return toBigDecimal((byte[])o);
                                   }
                                 }),

    ratioType = new LogicalType(Symbol.intern("ratio"),
                                BYTE_ARRAY,
                                asRatio,
                                new AFn() {
                                  public Object invoke(Object o) {
                                    return toByteArray((Ratio)o);
                                  }
                                },
                                new AFn() {
                                  public Object invoke(Object o) {
                                    return toRatio((byte[])o);
                                  }
                                }),

    keywordType = new LogicalType(Symbol.intern("keyword"),
                                  STRING,
                                  asKeyword,
                                  new AFn() {
                                    public Object invoke(Object o) {
                                      return Types.toString((Keyword)o);
                                    }
                                  },
                                  new AFn() {
                                    public Object invoke(Object o) {
                                      return toKeyword((String)o);
                                    }
                                  }),

    symbolType = new LogicalType(Symbol.intern("symbol"),
                                 STRING,
                                 asSymbol,
                                  new AFn() {
                                    public Object invoke(Object o) {
                                      return Types.toString((Symbol)o);
                                    }
                                  },
                                  new AFn() {
                                    public Object invoke(Object o) {
                                      return toSymbol((String)o);
                                    }
                                  }),

    byteBufferType = new LogicalType(Symbol.intern("byte-buffer"),
                                     BYTE_ARRAY,
                                     asByteBuffer,
                                     new AFn() {
                                       public Object invoke(Object o) {
                                         return toByteArray((ByteBuffer)o);
                                       }
                                     },
                                     new AFn() {
                                       public Object invoke(Object o) {
                                         return toByteBuffer((byte[])o);
                                       }
                                     });

  private final static LogicalType[] builtInLogicalTypes;

  static {
    builtInLogicalTypes = new LogicalType[BYTE_BUFFER+1];

    builtInLogicalTypes[STRING] = stringType;
    builtInLogicalTypes[INST] = instType;
    builtInLogicalTypes[UUID] = uuidType;
    builtInLogicalTypes[CHAR] = charType;
    builtInLogicalTypes[BIGINT] = bigIntType;
    builtInLogicalTypes[BIGDEC] = bigDecType;
    builtInLogicalTypes[RATIO] = ratioType;
    builtInLogicalTypes[KEYWORD] = keywordType;
    builtInLogicalTypes[SYMBOL] = symbolType;
    builtInLogicalTypes[BYTE_BUFFER] = byteBufferType;
  }

  private static void fillBuiltInLogicalTypes(LogicalType[] logicalTypes) {
    System.arraycopy(builtInLogicalTypes, 0, logicalTypes, 0, builtInLogicalTypes.length);
  }

  private static void fillBuiltInLogicalTypeSymbols(HashMap<Symbol,Integer> logicalTypesMap) {
    for (int i=0; i<builtInLogicalTypes.length; ++i) {
      LogicalType lt = builtInLogicalTypes[i];
      if (lt != null) {
        logicalTypesMap.put(lt.sym, i);
      }
    }
  }

  // Encodings
  public final static int
    PLAIN = 0,
    DICTIONARY = 1,
    FREQUENCY = 2,
    VLQ = 3,
    ZIG_ZAG = 4,
    PACKED_RUN_LENGTH = 5,
    DELTA = 6,
    INCREMENTAL = 7,
    DELTA_LENGTH = 8;

  private final static int[][] validEncodings;

  static {
    validEncodings = new int[-FIXED_LENGTH_BYTE_ARRAY][];
    validEncodings[i(BOOLEAN)] = new int[] {PLAIN, DICTIONARY, FREQUENCY};
    validEncodings[i(INT)] = new int[] {PLAIN, VLQ, ZIG_ZAG, PACKED_RUN_LENGTH, DELTA, DICTIONARY, FREQUENCY};
    validEncodings[i(LONG)] = new int[] {PLAIN, VLQ, ZIG_ZAG, DELTA, DICTIONARY, FREQUENCY};
    validEncodings[i(FLOAT)] = new int[] {PLAIN, DICTIONARY, FREQUENCY};
    validEncodings[i(DOUBLE)] = new int[] {PLAIN, DICTIONARY, FREQUENCY};
    validEncodings[i(BYTE_ARRAY)] = new int[] {PLAIN, INCREMENTAL, DELTA_LENGTH, DICTIONARY, FREQUENCY};
    validEncodings[i(FIXED_LENGTH_BYTE_ARRAY)] = new int[] {PLAIN, DICTIONARY, FREQUENCY};
  }

  private final static Symbol[] encodingSymbols;
  private final static HashMap<Symbol,Integer> encodings;

  final static Symbol PLAIN_SYM = Symbol.intern("plain");

  static {
    encodingSymbols = new Symbol[DELTA_LENGTH+1];
    encodingSymbols[PLAIN] = PLAIN_SYM;
    encodingSymbols[DICTIONARY] = Symbol.intern("dictionary");
    encodingSymbols[FREQUENCY] = Symbol.intern("frequency");
    encodingSymbols[VLQ] = Symbol.intern("vlq");
    encodingSymbols[ZIG_ZAG] = Symbol.intern("zig-zag");
    encodingSymbols[PACKED_RUN_LENGTH] = Symbol.intern("packed-run-length");
    encodingSymbols[DELTA] = Symbol.intern("delta");
    encodingSymbols[INCREMENTAL] = Symbol.intern("incremental");
    encodingSymbols[DELTA_LENGTH] = Symbol.intern("delta-length");

    encodings = new HashMap<Symbol,Integer>(2 * DELTA_LENGTH);
    for (int i=0; i<encodingSymbols.length; ++i) {
      encodings.put(encodingSymbols[i], i);
    }
  }

  private static IDecoderFactory getPrimitiveDecoderFactory(int type, int encoding) {
    switch (type) {
    case BOOLEAN: return BooleanPacked.decoderFactory;
    case INT: switch (encoding) {
      case PLAIN: return IntPlain.decoderFactory;
      case VLQ: return IntVLQ.decoderFactory;
      case ZIG_ZAG: return IntZigZag.decoderFactory;
      case PACKED_RUN_LENGTH: return IntPackedRunLength.decoderFactory;
      case DELTA: return IntPackedDelta.decoderFactory;
      }
    case LONG: switch (encoding) {
      case PLAIN: return LongPlain.decoderFactory;
      case VLQ: return LongVLQ.decoderFactory;
      case ZIG_ZAG: return LongZigZag.decoderFactory;
      case DELTA: return LongPackedDelta.decoderFactory;
      }
    case FLOAT: return FloatPlain.decoderFactory;
    case DOUBLE: return DoublePlain.decoderFactory;
    case BYTE_ARRAY: switch (encoding) {
      case PLAIN: return ByteArrayPlain.decoderFactory;
      case INCREMENTAL: return ByteArrayIncremental.decoderFactory;
      case DELTA_LENGTH: return ByteArrayDeltaLength.decoderFactory;
      }
    case FIXED_LENGTH_BYTE_ARRAY: return FixedLengthByteArrayPlain.decoderFactory;
    default: return null; // never reaches here
    }
  }

  private static IEncoder getPrimitiveEncoder(int type, int encoding) {
    switch (type) {
    case BOOLEAN: return new BooleanPacked.Encoder();
    case INT: switch (encoding) {
      case PLAIN: return new IntPlain.Encoder();
      case VLQ: return new IntVLQ.Encoder();
      case ZIG_ZAG: return new IntZigZag.Encoder();
      case PACKED_RUN_LENGTH: return new IntPackedRunLength.Encoder();
      case DELTA: return new IntPackedDelta.Encoder();
      }
    case LONG: switch (encoding) {
      case PLAIN: return new LongPlain.Encoder();
      case VLQ: return new LongVLQ.Encoder();
      case ZIG_ZAG: return new LongZigZag.Encoder();
      case DELTA: return new LongPackedDelta.Encoder();
      }
    case FLOAT: return new FloatPlain.Encoder();
    case DOUBLE: return new DoublePlain.Encoder();
    case BYTE_ARRAY: switch (encoding) {
      case PLAIN: return new ByteArrayPlain.Encoder();
      case INCREMENTAL: return new ByteArrayIncremental.Encoder();
      case DELTA_LENGTH: return new ByteArrayDeltaLength.Encoder();
      }
    case FIXED_LENGTH_BYTE_ARRAY: return new FixedLengthByteArrayPlain.Encoder();
    default: return null; // never reaches here
    }
  }

  // Compressions
  public final static int
    NONE = 0,
    DEFLATE = 1;

  final static Symbol
    NONE_SYM = Symbol.intern("none"),
    DEFLATE_SYM = Symbol.intern("deflate");

  public static IEncoder levelsEncoder(int maxLevel) {
    return new IntFixedBitWidthPackedRunLength.Encoder(Bytes.getBitWidth(maxLevel));
  }

  public static IIntDecoder levelsDecoder(ByteBuffer bb, int maxLevel) {
    return new IntFixedBitWidthPackedRunLength.Decoder(bb, Bytes.getBitWidth(maxLevel));
  }

  final static Charset utf8Charset = Charset.forName("UTF-8");

  public static byte[] toByteArray(String s) {
    if (s == null) {
      return null;
    }
    return s.getBytes(utf8Charset);
  }

  public static String toString(byte[] bs) {
    if (bs == null) {
      return null;
    }
    return new String(bs, utf8Charset);
  }

  public static Symbol toSymbol(String s) {
    return Symbol.intern(s);
  }

  public static String toString(Symbol sym) {
    String namespace = sym.getNamespace();
    String name = sym.getName();
    if (namespace == null) {
      return name;
    }
    return namespace + "/" + name;
  }

  public static Keyword toKeyword(String s) {
    return Keyword.intern(s);
  }

  public static String toString(Keyword k) {
    return toString(k.sym);
  }

  public static byte[] toByteArray(ByteBuffer bb) {
    if (bb == null) {
      return null;
    }
    int length = bb.limit() - bb.position();
    byte[] bs = new byte[length];
    bb.mark();
    bb.get(bs);
    bb.reset();
    return bs;
  }

  public static ByteBuffer toByteBuffer(byte[] bs) {
    if (bs == null) {
      return null;
    }
    return ByteBuffer.wrap(bs);
  }

  public static byte[] toByteArray(BigInt bi) {
   return  bi.toBigInteger().toByteArray();
  }

  public static BigInt toBigInt(byte[] bs) {
    return BigInt.fromBigInteger(new BigInteger(bs));
  }

  public static byte[] toByteArray(BigDecimal bd) {
    byte[] unscaledBigIntBytes = bd.unscaledValue().toByteArray();
    MemoryOutputStream mos = new MemoryOutputStream(unscaledBigIntBytes.length + 4);
    Bytes.writeFixedInt(mos, bd.scale());
    mos.write(unscaledBigIntBytes);
    return mos.buffer;
  }

  public static BigDecimal toBigDecimal(byte[] bs) {
    ByteBuffer bb = ByteBuffer.wrap(bs);
    int scale = Bytes.readFixedInt(bb);
    byte[] unscaledBigIntBytes = new byte[bs.length - 4];
    bb.get(unscaledBigIntBytes);
    return new BigDecimal(new BigInteger(unscaledBigIntBytes), scale);
  }

  public static byte[] toByteArray(Ratio r) {
    byte[] numeratorBytes = r.numerator.toByteArray();
    byte[] denominatorBytes = r.denominator.toByteArray();
    MemoryOutputStream mos = new MemoryOutputStream(numeratorBytes.length + denominatorBytes.length + 4);
    Bytes.writeFixedInt(mos, numeratorBytes.length);
    mos.write(numeratorBytes);
    mos.write(denominatorBytes);
    return mos.buffer;
  }

  public static Ratio toRatio(byte[] bs) {
    ByteBuffer bb = ByteBuffer.wrap(bs);
    int numeratorLength = Bytes.readFixedInt(bb);
    byte[] numeratorBytes = new byte[numeratorLength];
    byte[] denominatorBytes = new byte[bs.length - (numeratorLength + 4)];
    bb.get(numeratorBytes);
    bb.get(denominatorBytes);
    return new Ratio(new BigInteger(numeratorBytes), new BigInteger(denominatorBytes));
  }

  public static long toLong(Date d) {
    return d.getTime();
  }

  public static Date toDate(long l) {
    return new Date(l);
  }

  public static byte[] toByteArray(UUID uuid) {
    MemoryOutputStream mos = new MemoryOutputStream(16);
    Bytes.writeFixedLong(mos, uuid.getMostSignificantBits());
    Bytes.writeFixedLong(mos, uuid.getLeastSignificantBits());
    return mos.buffer;
  }

  public static UUID toUUID(byte[] bs) {
    ByteBuffer bb = ByteBuffer.wrap(bs);
    return new UUID(Bytes.readFixedLong(bb), Bytes.readFixedLong(bb));
  }

  private static final class LogicalType {
    final Symbol sym;
    final int baseType;
    final IFn coercionFn;
    final IFn toBaseTypeFn;
    final IFn fromBaseTypeFn;

    LogicalType(Symbol sym, int baseType, IFn coercionFn, IFn toBaseTypeFn, IFn fromBaseTypeFn) {
      this.sym = sym;
      this.baseType = baseType;
      this.coercionFn = coercionFn;
      this.toBaseTypeFn = toBaseTypeFn;
      this.fromBaseTypeFn = fromBaseTypeFn;
    }
  }

  private static int getMaxCustomType(IPersistentCollection customTypes) {
    int maxType = Integer.MIN_VALUE;
    for (ISeq s = RT.seq(customTypes); s != null; s = s.next()) {
      int type = ((CustomType)s.first()).type;
      if (type > maxType) {
        maxType = type;
      }
    }
    return maxType;
  }

  private static void fillCustomTypesConfSymbols(HashMap<Symbol, Integer> logicalTypesMap,
                                                 IPersistentMap customTypesConf,
                                                 IPersistentCollection customTypes) {
    int nextCustomType = Math.max(FIRST_CUSTOM_TYPE, getMaxCustomType(customTypes) + 1);
    ISeq keySeq = RT.keys(customTypesConf);
    while (keySeq != null) {
      Object o = keySeq.first();
      if (!(o instanceof Symbol)) {
        throw new IllegalArgumentException(String.format("Custom-type '%s' is not a symbol.", o));
      }
      Symbol sym = (Symbol) o;
      if (!logicalTypesMap.containsKey(sym)) {
        logicalTypesMap.put(sym, nextCustomType);
        nextCustomType += 1;
      }
      keySeq = keySeq.next();
    }
  }

  private static void fillCustomTypesSymbols(HashMap<Symbol, Integer> logicalTypesMap,
                                             IPersistentCollection customTypes) {
    for (ISeq s = RT.seq(customTypes); s != null; s = s.next()) {
      CustomType ct = (CustomType)s.first();
      logicalTypesMap.put(ct.sym, ct.type);
    }
  }

  private static int getMaxType(HashMap<Symbol, Integer> logicalTypesMap) {
    int maxType = Integer.MIN_VALUE;
    for (Integer i : logicalTypesMap.values()) {
      if (i > maxType) {
        maxType = i;
      }
    }
    return maxType;
  }

  private final static Keyword
    BASE_TYPE = Keyword.intern("base-type"),
    COERCION_FN = Keyword.intern("coercion-fn"),
    TO_BASE_TYPE_FN = Keyword.intern("to-base-type-fn"),
    FROM_BASE_TYPE_FN = Keyword.intern("from-base-type-fn");

  private final static String validKeysMessage =
    "Valid keys are :base-type, :coercion-fn, :to-base-type-fn, and :from-base-type-fn.";

  private static void checkTypeConfKeys(IPersistentMap typeConf) {
    ISeq keySeq = RT.keys(typeConf);
    while (keySeq != null) {
      Object o = keySeq.first();
      if (!(o instanceof Keyword)) {
        throw new IllegalArgumentException(String.format("Key '%s' is not a keyword. " + validKeysMessage, o));
      }
      Keyword k = (Keyword) o;
      if (!(k.equals(BASE_TYPE) || k.equals(COERCION_FN) || k.equals(TO_BASE_TYPE_FN) ||
            k.equals(FROM_BASE_TYPE_FN))) {
        String err = String.format("Key '%s' is not a valid custom-type field. " + validKeysMessage, k);
        throw new IllegalArgumentException(err);
      }
      keySeq = keySeq.next();
    }
  }

  private static Symbol getBaseSymbol(IPersistentMap typeConf) {
    Object o = typeConf.valAt(BASE_TYPE);
    if (o == null) {
      throw new IllegalArgumentException("Required field :base-type is missing.");
    }
    if (!(o instanceof Symbol)) {
      throw new IllegalArgumentException(String.format("Base type '%s' is not a symbol.", o));
    }
    return (Symbol)o;
  }

  private static IFn getLogicalTypeFn(IPersistentMap typeConf, Keyword kw) {
    Object o = typeConf.valAt(kw);
    if (o == null) {
      return null;
    }
    if (!(o instanceof IFn)) {
      throw new IllegalArgumentException(String.format("%s expects a function.", kw));
    }
    return (IFn)o;
  }

  private static Integer tryGetType(Symbol sym, HashMap<Symbol, Integer> logicalTypesMap) {
    Integer type = primitiveTypes.get(sym);
    if (type == null) {
      type = logicalTypesMap.get(sym);
    }
    return type;
  }

  private static LogicalType parseTypeConf(HashMap<Symbol, Integer> logicalTypesMap,
                                           Symbol typeSym,
                                           IPersistentMap typeConf) {
    try {
      checkTypeConfKeys(typeConf);
      Symbol baseSym = getBaseSymbol(typeConf);
      Integer baseType = tryGetType(baseSym, logicalTypesMap);
      if (baseType == null) {
        throw new IllegalArgumentException(String.format("Unknown base type '%s'.", baseSym));
      }
      return new LogicalType(typeSym,
                             baseType,
                             getLogicalTypeFn(typeConf, COERCION_FN),
                             getLogicalTypeFn(typeConf, TO_BASE_TYPE_FN),
                             getLogicalTypeFn(typeConf, FROM_BASE_TYPE_FN));
    } catch (Exception e) {
      throw new IllegalArgumentException(String.format("Error parsing custom-type '%s'.", typeSym), e);
    }
  }

  private static void fillCustomTypesConf(LogicalType[] logicalTypes,
                                          HashMap<Symbol, Integer> logicalTypesMap,
                                          IPersistentMap customTypesConf) {
    for (ISeq s = RT.seq(customTypesConf); s != null; s = s.next()) {
      IMapEntry entry = (IMapEntry)s.first();
      Symbol sym = (Symbol)entry.key();
      IPersistentMap typeConf = (IPersistentMap)entry.val();
      Integer type = logicalTypesMap.get(sym);
      logicalTypes[type] = parseTypeConf(logicalTypesMap, sym, typeConf);
    }
  }

  private static void fillEmptyCustomTypes(LogicalType[] logicalTypes,
                                           IPersistentCollection customTypes) {
    for (ISeq s = RT.seq(customTypes); s != null; s = s.next()) {
      CustomType ct = (CustomType)s.first();
      if (logicalTypes[ct.type] == null) {
        logicalTypes[ct.type] = new LogicalType(ct.sym, ct.baseType, null, null, null);
      }
    }
  }

  private static void flattenLogicalType(LogicalType[] logicalTypes, int type, boolean[] visited) {
    LogicalType logicalType = logicalTypes[type];
    if (logicalType == null) {
      return;
    }
    int baseType = logicalType.baseType;
    if (isPrimitive(baseType)) {
      return;
    }
    Arrays.fill(visited, false);
    visited[type] = true;
    LogicalType lt = logicalType;
    LinkedList<IFn> toBaseTypeFns = new LinkedList<IFn>();
    LinkedList<IFn> fromBaseTypeFns = new LinkedList<IFn>();
    if (lt.toBaseTypeFn != null) {
      toBaseTypeFns.addFirst(lt.toBaseTypeFn);
    }
    if (lt.fromBaseTypeFn != null) {
      fromBaseTypeFns.addLast(lt.fromBaseTypeFn);
    }
    while (!isPrimitive(baseType)) {
      if (visited[baseType]) {
        throw new IllegalArgumentException(String.format("Loop detected for custom-type '%s'.",
                                                         logicalType.sym));
      }
      visited[baseType] = true;
      lt = logicalTypes[baseType];
      if (lt.toBaseTypeFn != null) {
        toBaseTypeFns.addFirst(lt.toBaseTypeFn);
      }
      if (lt.fromBaseTypeFn != null) {
        fromBaseTypeFns.addLast(lt.fromBaseTypeFn);
      }
      baseType = lt.baseType;
    }
    logicalTypes[type] = new LogicalType(logicalType.sym,
                                         baseType,
                                         logicalType.coercionFn,
                                         Functions.comp(toBaseTypeFns.toArray(new IFn[]{})),
                                         Functions.comp(fromBaseTypeFns.toArray(new IFn[]{})));
  }

  private static void flattenLogicalTypes(LogicalType[] logicalTypes) {
    boolean[] visited = new boolean[logicalTypes.length];
    for (int i=0; i<logicalTypes.length; ++i) {
      flattenLogicalType(logicalTypes, i, visited);
    }
  }

  public static Types create(IPersistentMap customTypesConf, IPersistentCollection customTypes) {
    int estimatedNumTypes = builtInLogicalTypes.length + RT.count(customTypesConf);
    HashMap<Symbol,Integer> logicalTypesMap = new HashMap<Symbol,Integer>(2 * estimatedNumTypes);
    fillBuiltInLogicalTypeSymbols(logicalTypesMap);
    fillCustomTypesSymbols(logicalTypesMap, customTypes);
    fillCustomTypesConfSymbols(logicalTypesMap, customTypesConf, customTypes);
    int maxType = getMaxType(logicalTypesMap);
    LogicalType[] logicalTypes = new LogicalType[maxType+1];
    fillBuiltInLogicalTypes(logicalTypes);
    fillCustomTypesConf(logicalTypes, logicalTypesMap, customTypesConf);
    fillEmptyCustomTypes(logicalTypes, customTypes);
    flattenLogicalTypes(logicalTypes);
    return new Types(logicalTypesMap, logicalTypes);
  }

  private final HashMap<Symbol,Integer> logicalTypesMap;
  private final LogicalType[] logicalTypes;

  private Types(HashMap<Symbol,Integer> logicalTypesMap, LogicalType[] logicalTypes) {
    this.logicalTypesMap = logicalTypesMap;
    this.logicalTypes = logicalTypes;
  }

  public Symbol getTypeSymbol(int type) {
    if (isPrimitive(type)) {
      return primitiveTypeSymbols[i(type)];
    }
    return logicalTypes[type].sym;
  }

  public int getType(Symbol sym) {
    Integer type = tryGetType(sym, logicalTypesMap);
    if (type == null) {
      throw new IllegalArgumentException("Unknown type: '" + sym + "'.");
    }
    return type;
  }

  public Symbol getEncodingSymbol(int encoding) {
    return encodingSymbols[encoding];
  }

  public int getEncoding(Symbol sym) {
    Integer encoding = encodings.get(sym);
    if (encoding == null) {
      throw new IllegalArgumentException("Unknown encoding: '" + sym + "'.");
    }
    return encoding;
  }

  public int getEncoding(int type, Symbol sym) {
    int encoding = getEncoding(sym);
    int[] encodingsForType = listEncodingsForType(type);
    for (int i=0; i<encodingsForType.length; ++i) {
      if (encodingsForType[i] == encoding) {
        return encoding;
      }
    }
    throw new IllegalArgumentException(String.format("Unsupported encoding '%s' for type '%s'.",
                                                     sym, getTypeSymbol(type)));
  }

  public Symbol getCompressionSymbol(int compression) {
    if (compression == NONE) {
      return NONE_SYM;
    } else /* if (compression == DEFLATE) */ {
      return DEFLATE_SYM;
    }
  }

  public int getCompression(Symbol sym) {
    if (sym.equals(NONE_SYM)) {
      return NONE;
    } else if (sym.equals(DEFLATE_SYM)) {
      return DEFLATE;
    }
    throw new IllegalArgumentException("Unknown compression: '" + sym + "'.");
  }

  public int[] listEncodingsForType(int type) {
    if (isPrimitive(type)) {
      return validEncodings[i(type)];
    }
    int baseType = logicalTypes[type].baseType;
    return validEncodings[i(baseType)];
  }

  public IEncoder getEncoder(int type, int encoding) {
    if (isPrimitive(type)) {
      return getPrimitiveEncoder(type, encoding);
    }
    LogicalType lt = logicalTypes[type];
    final IEncoder enc = getPrimitiveEncoder(lt.baseType, encoding);
    final IFn toBaseTypeFn = lt.toBaseTypeFn;
    return new IEncoder() {
      public void encode(Object o) { enc.encode(toBaseTypeFn.invoke(o)); }
      public int numEncodedValues() { return enc.numEncodedValues(); }
      public void reset() { enc.reset(); }
      public void finish() { enc.finish(); }
      public int length() { return enc.length(); }
      public int estimatedLength() { return enc.estimatedLength(); }
      public void writeTo(MemoryOutputStream mos) { mos.write(enc); }
    };
  }

  public IDecoderFactory getDecoderFactory(int type, int encoding) {
    if (isPrimitive(type)) {
      return getPrimitiveDecoderFactory(type, encoding);
    }
    LogicalType lt = logicalTypes[type];
    final IDecoderFactory decoderFactory = getPrimitiveDecoderFactory(lt.baseType, encoding);
    final IFn fromBaseTypeFn = lt.fromBaseTypeFn;
    return new IDecoderFactory() {
      public IDecoder create(ByteBuffer bb) {
        final IDecoder dec = decoderFactory.create(bb);
        return new IDecoder() {
          public Object decode() { return fromBaseTypeFn.invoke(dec.decode()); }
          public int numEncodedValues() { return dec.numEncodedValues(); }
        };
      }
    };
  }

  public IFn getCoercionFn(final int type) {
    final IFn coercionFn = isPrimitive(type)? primitiveCoercionFns[i(type)] : logicalTypes[type].coercionFn;
    return new AFn() {
      public Object invoke(Object o) {
        try {
          return coercionFn.invoke(o);
        } catch (Exception e) {
          throw new IllegalArgumentException(String.format("Could not coerce '%s' into a %s.",
                                                           o, getTypeSymbol(type)));
        }
      }
    };
  }

  public IFn getToBaseTypeFn(int type) {
    if (isPrimitive(type)) {
      return null;
    }
    return logicalTypes[type].toBaseTypeFn;
  }

  public IFn getFromBaseTypeFn(int type) {
    if (isPrimitive(type)) {
      return null;
    }
    return logicalTypes[type].fromBaseTypeFn;
  }

  public int getPrimitiveType(int type) {
    if (isPrimitive(type)) {
      return type;
    }
    return logicalTypes[type].baseType;
  }

}
