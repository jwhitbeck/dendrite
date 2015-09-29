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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class Stripe {

  public interface Fn {
    boolean invoke(Object record, Object[] buffer);
  }

  public static Fn getFn(Types types, Schema schema, final IFn mapFn, final IFn errorHandlerFn,
                         boolean isIgnoreExtraFields) {
    final StripeFn stripeFn = getStripeFn(new Context(types, isIgnoreExtraFields),
                                          schema, PersistentVector.EMPTY);
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

  private static final Object notFound = new Object();

  private static class Context {
    Types types;
    boolean isIgnoreExtraFields;

    Context(Types types, boolean isIgnoreExtraFields) {
      this.types = types;
      this.isIgnoreExtraFields = isIgnoreExtraFields;
    }
  }

  private interface StripeFn {
    void invoke(Object[] buffer, Object obj, boolean isParentNil, int repetitionLevel, int definitionLevel);
  }

  private static StripeFn getStripeFn(Context context, Schema schema, IPersistentVector parents) {
    if (schema instanceof Schema.Column) {
      if (schema.repetition == Schema.OPTIONAL) {
        return getOptionalValueStripeFn(context, (Schema.Column)schema, parents);
      } else /* if (schema.repetition == Schema.REQUIRED) */ {
        return getRequiredValueStripeFn(context, (Schema.Column)schema, parents);
      }
    } else if (schema instanceof Schema.Record) {
      if (schema.repetition == Schema.OPTIONAL) {
        return getOptionalRecordStripeFn(context, (Schema.Record)schema, parents);
      } else /* if (schema.repetition == Schema.REQUIRED) */ {
        return getRequiredRecordStripeFn(context, (Schema.Record)schema, parents);
      }
    } else /* if (schema instanceof Schema.Collection) */ {
      if (schema.repetition == Schema.MAP) {
        return getMapStripeFn(context, (Schema.Collection)schema, parents);
      } else {
        return getRepeatedStripeFn(context, (Schema.Collection)schema, parents);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static void appendRepeated(Object[] buffer, int colIdx, Object v) {
    List<Object> list = (List<Object>)buffer[colIdx];
    if (list == null) {
      list = new ArrayList<Object>();
      buffer[colIdx] = list;
    }
    list.add(v);
  }

  private static StripeFn getOptionalValueStripeFn(Context context, Schema.Column column,
                                                   final IPersistentVector parents) {
    Types types = context.types;
    final IFn coercionFn = types.getCoercionFn(column.type);
    final IFn toBaseTypeFn = Types.USE_IN_COLUMN_LOGICAL_TYPES? null : types.getToBaseTypeFn(column.type);
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
            buffer[colIdx] = (toBaseTypeFn == null)? v : toBaseTypeFn.invoke(v);
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
            LeveledValue lv = new LeveledValue(repetitionLevel,
                                               maxDefinitionLevel,
                                               (toBaseTypeFn == null)? v : toBaseTypeFn.invoke(v));
            appendRepeated(buffer, colIdx, lv);
          }
        }
      };
    }
  }

  private static StripeFn getRequiredValueStripeFn(Context context, Schema.Column column,
                                                   final IPersistentVector parents) {
    Types types = context.types;
    final IFn coercionFn = types.getCoercionFn(column.type);
    final IFn toBaseTypeFn = Types.USE_IN_COLUMN_LOGICAL_TYPES? null : types.getToBaseTypeFn(column.type);
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
            buffer[colIdx] = (toBaseTypeFn == null)? v : toBaseTypeFn.invoke(v);
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
            LeveledValue lv = new LeveledValue(repetitionLevel,
                                               maxDefinitionLevel,
                                               (toBaseTypeFn == null) ? v : toBaseTypeFn.invoke(v));
            appendRepeated(buffer, colIdx, lv);
          }
        }
      };
    }
  }

  private static void checkNoExtraFields(Set<Keyword> fieldNameSet, Object rec, IPersistentVector parents) {
    for (ISeq s = RT.keys(rec); s != null; s = s.next()) {
      Keyword k = (Keyword)s.first();
      if (!fieldNameSet.contains(k)) {
        throw new IllegalArgumentException(
            String.format("Field '%s' at path '%s' is not in schema", k, parents));
      }
    }
  }

  private static StripeFn getOptionalRecordStripeFn(Context context, Schema.Record record,
                                                    final IPersistentVector parents) {
    Schema.Field[] fields = record.fields;
    final StripeFn[] fieldStripeFns = new StripeFn[fields.length];
    final Keyword[] fieldNames = new Keyword[fields.length];
    for (int i=0; i<fields.length; ++i) {
      Schema.Field field = fields[i];
      fieldNames[i] = field.name;
      fieldStripeFns[i] = getStripeFn(context, field.value, parents.cons(field.name));
    }
    if (!context.isIgnoreExtraFields) {
      final Set<Keyword> fieldNameSet = new HashSet<Keyword>(fields.length * 2);
      for (Keyword fieldName : fieldNames) {
        fieldNameSet.add(fieldName);
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
          if (!isEmpty) {
            checkNoExtraFields(fieldNameSet, rec, parents);
          }
        }
      };
    } else {
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
  }

  private static StripeFn getRequiredRecordStripeFn(Context context, Schema.Record record,
                                                    final IPersistentVector parents) {
    Schema.Field[] fields = record.fields;
    final StripeFn[] fieldStripeFns = new StripeFn[fields.length];
    final Keyword[] fieldNames = new Keyword[fields.length];
    for (int i=0; i<fields.length; ++i) {
      Schema.Field field = fields[i];
      fieldNames[i] = field.name;
      fieldStripeFns[i] = getStripeFn(context, field.value, parents.cons(field.name));
    }
    if (!context.isIgnoreExtraFields) {
      final Set<Keyword> fieldNameSet = new HashSet<Keyword>(fields.length * 2);
      for (Keyword fieldName : fieldNames) {
        fieldNameSet.add(fieldName);
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
          if (!isNil) {
            checkNoExtraFields(fieldNameSet, rec, parents);
          }
        }
      };
    } else {
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
  }

  private static IFn keyValueFn = new AFn() {
      public Object invoke(Object o) {
        Map.Entry e = (Map.Entry)o;
        return new PersistentArrayMap(new Object[]{Schema.KEY, e.getKey(), Schema.VAL, e.getValue()});
      }
    };

  private static ISeq seq(Object o) {
    if (o == notFound) {
      return null;
    }
    return RT.seq(o);
  }

  private static StripeFn getMapStripeFn(Context context, Schema.Collection map,
                                         final IPersistentVector parents) {
    final StripeFn repeatedStripeFn = getStripeFn(context, map.withRepetition(Schema.LIST), parents);
    return new StripeFn() {
      public void invoke(Object[] buffer, Object mapObj, boolean isParentNil, int repetitionLevel,
                         int definitionLevel) {
        repeatedStripeFn.invoke(buffer, (mapObj == notFound)? notFound : Utils.map(keyValueFn, mapObj),
                                isParentNil, repetitionLevel, definitionLevel);
      }
    };
  }

  private static StripeFn getRepeatedStripeFn(Context context, Schema.Collection coll,
                                              final IPersistentVector parents) {
    final StripeFn repeatedElementStripeFn = getStripeFn(context, coll.repeatedSchema, parents.cons(null));
    final int curRepetitionLevel = coll.repetitionLevel;
    final int curDefinitionLevel = coll.definitionLevel;
    return new StripeFn() {
      public void invoke(Object[] buffer, Object repeatedValues, boolean isParentNil, int repetitionLevel,
                         int definitionLevel) {
        if (repeatedValues == null || repeatedValues == notFound) {
          repeatedElementStripeFn.invoke(buffer, notFound, true, repetitionLevel, definitionLevel);
        } else {
          ISeq s;
          try {
            s = seq(repeatedValues);
          } catch (Exception e) {
            throw new IllegalArgumentException(
                String.format("Could not iterate over value at path '%s'", parents), e);
          }
          if (s == null) { // Sequence was empty but not null
            repeatedElementStripeFn.invoke(buffer, notFound, true, repetitionLevel, curDefinitionLevel);
          } else { // Sequence has a least one elements
            repeatedElementStripeFn.invoke(buffer, s.first(), isParentNil, repetitionLevel,
                                           curDefinitionLevel + 1);
            for (ISeq t = s.next(); t != null; t = t.next()) {
              repeatedElementStripeFn.invoke(buffer, t.first(), isParentNil, curRepetitionLevel,
                                             curDefinitionLevel + 1);
            }
          }
        }
      }
    };
  }

}
