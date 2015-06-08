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
import clojure.lang.IFn;
import clojure.lang.IPersistentVector;
import clojure.lang.ISeq;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;
import clojure.lang.PersistentVector;
import clojure.lang.RT;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class Stripe {

  private static final Object notFound = new Object();

  public interface Fn {
    boolean invoke(Object record, Object[] buffer);
  }

  public static Fn getFn(Types types, Schema schema, final IFn mapFn, final IFn errorHandlerFn) {
    final StripeFn stripeFn = getStripeFn(types, schema, PersistentVector.EMPTY);
    if (mapFn == null) {
      return new Fn() {
        public boolean invoke(Object record, Object[] buffer) {
          try {
            stripeFn.invoke(buffer, record, false, 0, 0);
            return true;
          } catch (Exception e) {
            if (errorHandlerFn != null) {
              errorHandlerFn.invoke(record, e);
              return false;
            } else {
              throw new IllegalArgumentException(String.format("Failed to stripe record '%s'", record), e);
            }
          }
        }
      };
    } else {
      return new Fn() {
        public boolean invoke(Object record, Object[] buffer) {
          try {
            stripeFn.invoke(buffer, mapFn.invoke(record), false, 0, 0);
            return true;
          } catch (Exception e) {
            if (errorHandlerFn != null) {
              errorHandlerFn.invoke(record, e);
              return false;
            } else {
              throw new IllegalArgumentException(String.format("Failed to stripe record '%s'", record), e);
            }
          }
        }
      };
    }
  }

  interface StripeFn {
    void invoke(Object[] buffer, Object obj, boolean isParentNil, int repetitionLevel, int definitionLevel);
  }

  static StripeFn getStripeFn(Types types, Schema schema, IPersistentVector parents) {
    if (schema instanceof Schema.Column) {
      if (schema.repetition == Schema.OPTIONAL) {
        return getOptionalValueStripeFn(types, (Schema.Column)schema, parents);
      } else /* if (schema.repetition == Schema.REQUIRED) */ {
        return getRequiredValueStripeFn(types, (Schema.Column)schema, parents);
      }
    } else if (schema instanceof Schema.Record) {
      if (schema.repetition == Schema.OPTIONAL) {
        return getOptionalRecordStripeFn(types, (Schema.Record)schema, parents);
      } else /* if (schema.repetition == Schema.REQUIRED) */ {
        return getRequiredRecordStripeFn(types, (Schema.Record)schema, parents);
      }
    } else /* if (schema instanceof Schema.Collection) */ {
      if (schema.repetition == Schema.MAP) {
        return getMapStripeFn(types, (Schema.Collection)schema, parents);
      } else {
        return getRepeatedStripeFn(types, (Schema.Collection)schema, parents);
      }
    }
  }

  @SuppressWarnings("unchecked")
  static void appendRepeated(Object[] buffer, int colIdx, Object v) {
    List<Object> list = (List<Object>)buffer[colIdx];
    if (list == null) {
      list = new ArrayList<Object>();
      buffer[colIdx] = list;
    }
    list.add(v);
  }

  static StripeFn getOptionalValueStripeFn(Types types, Schema.Column column,
                                           final IPersistentVector parents) {
    final IFn coercionFn = types.getCoercionFn(column.type);
    final int colIdx = column.columnIndex;
    final int maxDefinitionLevel = column.definitionLevel;
    if (column.repetitionLevel == 0) {
      return new StripeFn() {
        public void invoke(Object[] buffer, Object val, boolean isParentNil, int repetitionLevel,
                           int definitionLevel) {
          if (val == null || val == notFound) {
            buffer[colIdx] = null;
          } else {
            Object v;
            try {
              v = coercionFn.invoke(val);
            } catch (Exception e) {
              throw new IllegalArgumentException(
                 String.format("Could not coerce value at path '%s'", parents), e);
            }
            buffer[colIdx] = v;
          }
        }
      };
    } else {
      return new StripeFn() {
        public void invoke(Object[] buffer, Object val, boolean isParentNil, int repetitionLevel,
                           int definitionLevel) {
          if (val == null || val == notFound) {
            appendRepeated(buffer, colIdx, new LeveledValue(repetitionLevel, definitionLevel, null));
          } else {
            Object v;
            try {
              v = coercionFn.invoke(val);
            } catch (Exception e) {
              throw new IllegalArgumentException(
                 String.format("Could not coerce value at path '%s'", parents), e);
            }
            appendRepeated(buffer, colIdx, new LeveledValue(repetitionLevel, maxDefinitionLevel, v));
          }
        }
      };
    }
  }

  static StripeFn getRequiredValueStripeFn(Types types, Schema.Column column,
                                           final IPersistentVector parents) {
    final IFn coercionFn = types.getCoercionFn(column.type);
    final int colIdx = column.columnIndex;
    final int maxDefinitionLevel = column.definitionLevel;
    if (column.repetitionLevel == 0) {
      return new StripeFn() {
        public void invoke(Object[] buffer, Object val, boolean isParentNil, int repetitionLevel,
                           int definitionLevel) {
          if (isParentNil) {
            buffer[colIdx] = null;
          } else if (val == null || val == notFound) {
            throw new IllegalArgumentException(
               String.format("Required value at path '%s' is missing", parents));
          } else {
            Object v;
            try {
              v = coercionFn.invoke(val);
            } catch (Exception e) {
              throw new IllegalArgumentException(
                 String.format("Could not coerce value at path '%s'", parents), e);
            }
            buffer[colIdx] = v;
          }
        }
      };
    } else {
      return new StripeFn() {
        public void invoke(Object[] buffer, Object val, boolean isParentNil, int repetitionLevel,
                           int definitionLevel) {
          if (isParentNil) {
            appendRepeated(buffer, colIdx, new LeveledValue(repetitionLevel, definitionLevel, null));
          } else if (val == null || val == notFound) {
            throw new IllegalArgumentException(
               String.format("Required value at path '%s' is missing", parents));
          } else {
            Object v;
            try {
              v = coercionFn.invoke(val);
            } catch (Exception e) {
              throw new IllegalArgumentException(
                 String.format("Could not coerce value at path '%s'", parents), e);
            }
            appendRepeated(buffer, colIdx, new LeveledValue(repetitionLevel, maxDefinitionLevel, v));
          }
        }
      };
    }
  }

  static StripeFn getOptionalRecordStripeFn(Types types, Schema.Record record,
                                            final IPersistentVector parents) {
    Schema.Field[] fields = record.fields;
    final StripeFn[] fieldStripeFns = new StripeFn[fields.length];
    final Keyword[] fieldNames = new Keyword[fields.length];
    for (int i=0; i<fields.length; ++i) {
      Schema.Field field = fields[i];
      fieldNames[i] = field.name;
      fieldStripeFns[i] = getStripeFn(types, field.value, parents.cons(field.name));
    }
    return new StripeFn() {
      public void invoke(Object[] buffer, Object rec, boolean isParentNil, int repetitionLevel,
                         int definitionLevel) {
        boolean isEmpty = (rec == notFound);
        int defLevel = isEmpty? definitionLevel : definitionLevel + 1;
        for (int i=0; i<fieldNames.length; ++i) {
          fieldStripeFns[i].invoke(buffer, RT.get(rec, fieldNames[i], notFound), isEmpty, repetitionLevel,
                                   defLevel);
        }
      }
    };
  }

  static StripeFn getRequiredRecordStripeFn(Types types, Schema.Record record,
                                            final IPersistentVector parents) {
    Schema.Field[] fields = record.fields;
    final StripeFn[] fieldStripeFns = new StripeFn[fields.length];
    final Keyword[] fieldNames = new Keyword[fields.length];
    for (int i=0; i<fields.length; ++i) {
      Schema.Field field = fields[i];
      fieldNames[i] = field.name;
      fieldStripeFns[i] = getStripeFn(types, field.value, parents.cons(field.name));
    }
    return new StripeFn() {
      public void invoke(Object[] buffer, Object rec, boolean isParentNil, int repetitionLevel,
                         int definitionLevel) {
        boolean isNil = (rec == notFound);
        if (isNil && !isParentNil) {
          throw new IllegalArgumentException(
            String.format("Required record at path '%s' is missing", parents));
        }
        for (int i=0; i<fieldNames.length; ++i) {
          fieldStripeFns[i].invoke(buffer, RT.get(rec, fieldNames[i], notFound), isNil, repetitionLevel,
                                   definitionLevel);
        }
      }
    };
  }

  static IFn keyValueFn = new AFn() {
      public Object invoke(Object o) {
        Map.Entry e = (Map.Entry)o;
        return new PersistentArrayMap(new Object[]{Schema.KEY, e.getKey(), Schema.VAL, e.getValue()});
      }
    };

  static ISeq seq(Object o) {
    if (o == notFound) {
      return null;
    }
    return RT.seq(o);
  }

  static StripeFn getMapStripeFn(Types types, Schema.Collection map, final IPersistentVector parents) {
    final StripeFn repeatedStripeFn = getStripeFn(types, map.withRepetition(Schema.LIST), parents);
    return new StripeFn() {
      public void invoke(Object[] buffer, Object mapObj, boolean isParentNil, int repetitionLevel,
                         int definitionLevel) {
        ISeq entries;
        try {
          entries = seq(mapObj);
        } catch (Exception e) {
          throw new IllegalArgumentException(
            String.format("Could not iterate over value at path '%s'", parents), e);
        }
        if (entries == null) {
          repeatedStripeFn.invoke(buffer, null, true, repetitionLevel, definitionLevel);
        } else {
          repeatedStripeFn.invoke(buffer, Utils.map(keyValueFn, entries), isParentNil, repetitionLevel,
                                  definitionLevel);
        }
      }
    };
  }

  static StripeFn getRepeatedStripeFn(Types types, Schema.Collection coll, final IPersistentVector parents) {
    final StripeFn repeatedElementStripeFn = getStripeFn(types, coll.repeatedSchema, parents.cons(null));
    final int curRepetitionLevel = coll.repetitionLevel;
    final int curDefinitionLevel = coll.definitionLevel;
    return new StripeFn() {
      public void invoke(Object[] buffer, Object repeatedValues, boolean isParentNil, int repetitionLevel,
                         int definitionLevel) {
        ISeq s;
        try {
          s = seq(repeatedValues);
        } catch (Exception e) {
          throw new IllegalArgumentException(
             String.format("Could not iterate over value at path '%s'", parents), e);
        }
        if (s == null) {
          repeatedElementStripeFn.invoke(buffer, notFound, true, repetitionLevel, definitionLevel);
        } else {
          repeatedElementStripeFn.invoke(buffer, s.first(), isParentNil, repetitionLevel, curDefinitionLevel);
          for (ISeq t = s.next(); t != null; t = t.next()) {
            repeatedElementStripeFn.invoke(buffer, t.first(), isParentNil, curRepetitionLevel,
                                           curDefinitionLevel);
          }
        }
      }
    };
  }

}
