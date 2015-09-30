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
import clojure.lang.IEditableCollection;
import clojure.lang.IFn;
import clojure.lang.IPersistentCollection;
import clojure.lang.ISeq;
import clojure.lang.ITransientCollection;
import clojure.lang.ITransientMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;
import clojure.lang.PersistentHashSet;
import clojure.lang.PersistentList;
import clojure.lang.PersistentVector;
import clojure.lang.RT;

import java.util.ListIterator;

public final class Assemble {

  public interface Fn {
    Object invoke(ListIterator[] iterators);
  }

  public static Fn getFn(Types types, Schema schema) {
    if (schema instanceof Schema.Record) {
      return getRecordFn(types, (Schema.Record)schema);
    } else if (schema instanceof Schema.Column) {
      if (schema.presence < 0) {
        return getMissingValueFn(schema);
      } else if (schema.repetitionLevel == 0) {
        return getNonRepeatedValueFn(types, (Schema.Column)schema);
      } else {
        return getRepeatedValueFn(types, (Schema.Column)schema);
      }
    } else /* if (schema instanceof Schema.Collection) */ {
      if (schema.presence < 0) {
        return getMissingValueFn(schema);
      } else {
        Schema.Collection coll = (Schema.Collection)schema;
        if (coll.repetition == Schema.MAP) {
          return getMapFn(types, coll);
        } else {
          return getRepeatedFn(types, coll);
        }
      }
    }
  }

  private static Fn getMissingValueFn(Schema schema) {
    IFn fn = schema.fn;
    final Object v = (fn != null)? fn.invoke(null) : null;
    return new Fn() {
      public Object invoke(ListIterator[] iterators) {
        return v;
      }
    };
  }

  private static Fn getNonRepeatedValueFn(Types types, Schema.Column column) {
    final int colIdx = column.queryColumnIndex;
    final IFn fromBaseTypeFn;
    if (column.hasDictionary() || Types.USE_IN_COLUMN_LOGICAL_TYPES) {
      // NOTE: For columns with dictionaries, the column.fn is applied by the column decoder for greater
      // efficiency.
      fromBaseTypeFn = null;
    } else {
      fromBaseTypeFn = types.getFromBaseTypeFn(column.type, column.fn);
    }
    if (fromBaseTypeFn == null) {
      return new Fn() {
        public Object invoke(ListIterator[] iterators) {
          return iterators[colIdx].next();
        }
      };
    } else if (column.definitionLevel == 0) {
      return new Fn() {
        public Object invoke(ListIterator[] iterators) {
          return fromBaseTypeFn.invoke(iterators[colIdx].next());
        }
      };
    } else {
      final DelayedNullValue delayedNullValue = DelayedNullValue.withFn(column.fn);
      return new Fn() {
        public Object invoke(ListIterator[] iterators) {
          Object v = iterators[colIdx].next();
          if (v == null) {
            return delayedNullValue.get();
          } else {
            return fromBaseTypeFn.invoke(v);
          }
        }
      };
    }
  }

  @SuppressWarnings("unchecked")
  private static Fn getRepeatedValueFn(Types types, Schema.Column column) {
    final int colIdx = column.queryColumnIndex;
    final IFn fromBaseTypeFn;
    if (column.hasDictionary() || Types.USE_IN_COLUMN_LOGICAL_TYPES) {
      // NOTE: For columns with dictionaries, the column.fn is applied by the column decoder for greater
      // efficiency.
      fromBaseTypeFn = null;
    } else {
      fromBaseTypeFn = types.getFromBaseTypeFn(column.type, column.fn);
    }
    if (fromBaseTypeFn == null) {
      return new Fn() {
        public Object invoke(ListIterator[] iterators) {
          ListIterator<LeveledValue> iterator = iterators[colIdx];
          LeveledValue lv = iterator.next();
          return lv.value;
        }
      };
    } else {
      final DelayedNullValue delayedNullValue = DelayedNullValue.withFn(column.fn);
      final int enclosingCollectionMaxDefinitionLevel = column.enclosingCollectionMaxDefinitionLevel;
      return new Fn() {
        public Object invoke(ListIterator[] iterators) {
          ListIterator<LeveledValue> iterator = iterators[colIdx];
          LeveledValue lv = iterator.next();
          if (lv.value == null) {
            if (lv.definitionLevel == enclosingCollectionMaxDefinitionLevel + 1) {
              return delayedNullValue.get();
            } else {
              return null;
            }
          } else {
            return fromBaseTypeFn.invoke(lv.value);
          }
        }
      };
    }
  }

  private interface RecordConstructorFn {
    IPersistentCollection invoke(ListIterator[] iterators);
  }

  private static RecordConstructorFn getRecordConstructorFn(final Keyword[] fieldNames,
                                                            final Fn[] fieldAssemblyFns) {
    final PersistentRecord.Factory factory = new PersistentRecord.Factory(fieldNames);
    final int n = fieldAssemblyFns.length;
    return new RecordConstructorFn() {
      public IPersistentCollection invoke(ListIterator[] iterators) {
        Object[] vals = new Object[n];
        for (int i=0; i<n; ++i) {
          Object v = fieldAssemblyFns[i].invoke(iterators);
          vals[i] = (v == null)? PersistentRecord.UNDEFINED : v;
        }
        return factory.create(vals);
      }
    };
  }

  private static Fn getRecordFn(Types types, Schema.Record record) {
    Schema.Field[] fields = record.fields;
    int n = fields.length;
    final Keyword[] fieldNames = new Keyword[n];
    final Fn[] fieldAssemblyFns = new Fn[n];
    for (int i=0; i<n; ++i) {
      Schema.Field field = fields[i];
      fieldNames[i] = field.name;
      fieldAssemblyFns[i] = getFn(types, field.value);
    }
    final RecordConstructorFn recordConstructorFn = getRecordConstructorFn(fieldNames, fieldAssemblyFns);
    if (record.fn == null) {
      return new Fn() {
        public Object invoke(ListIterator[] iterators) {
          IPersistentCollection rec = recordConstructorFn.invoke(iterators);
          if (rec.count() == 0) {
            return null;
          } else {
            return rec;
          }
        }
      };
    } else {
      final IFn fn = record.fn;
      final DelayedNullValue delayedNullValue = DelayedNullValue.withFn(fn);
      return new Fn() {
        public Object invoke(ListIterator[] iterators) {
          IPersistentCollection rec = recordConstructorFn.invoke(iterators);
          if (rec.count() == 0) {
            return delayedNullValue.get();
          } else {
            return fn.invoke(rec);
          }
        }
      };
    }
  }

  @SuppressWarnings("unchecked")
  private static int getNextRepetitionLevel(ListIterator[] iterators, int colIdx) {
    ListIterator<LeveledValue> i = iterators[colIdx];
    if (!i.hasNext()) {
      return 0;
    }
    LeveledValue lv = i.next();
    i.previous();
    return lv.repetitionLevel;
  }

  @SuppressWarnings("unchecked")
  private static int getNextDefinitionLevel(ListIterator[] iterators, int colIdx) {
    ListIterator<LeveledValue> i = iterators[colIdx];
    if (!i.hasNext()) {
      return 0;
    }
    LeveledValue lv = i.next();
    i.previous();
    return lv.definitionLevel;
  }

  private static final IFn asListFn = new AFn() {
      public Object invoke(Object o) {
        if (o == null) {
          return null;
        }
        ISeq s = RT.seq(o);
        if (s == null) {
          return PersistentList.EMPTY;
        }
        return RT.seq(o);
      }
    };

  private static Fn getRepeatedFn(Types types, Schema.Collection coll) {
    final int leafColumnIndex = coll.leafColumnIndex;
    final int repetitionLevel = coll.repetitionLevel;
    final int definitionLevel = coll.definitionLevel;
    final IEditableCollection emptyColl;
    if (coll.repetition == Schema.SET) {
      emptyColl = PersistentHashSet.EMPTY;
    } else {
      emptyColl = PersistentVector.EMPTY;
    }
    final Fn repeatedElemFn = getFn(types, coll.repeatedSchema);
    final Fn repeatedFn = new Fn() {
        public Object invoke(ListIterator[] iterators) {
          int leafDefinitionLevel = getNextDefinitionLevel(iterators, leafColumnIndex);
          Object firstObject = repeatedElemFn.invoke(iterators);
          if (firstObject == null) {
            if (leafDefinitionLevel < definitionLevel) {
              return null;
            } else if (leafDefinitionLevel == definitionLevel) {
              return emptyColl;
            }
          }
          ITransientCollection tr = emptyColl.asTransient();
          tr = tr.conj(firstObject);
          int leafRepetitionLevel = getNextRepetitionLevel(iterators, leafColumnIndex);
          while (repetitionLevel <= leafRepetitionLevel) {
            Object nextRecord = repeatedElemFn.invoke(iterators);
            tr = tr.conj(nextRecord);
            leafRepetitionLevel = getNextRepetitionLevel(iterators, leafColumnIndex);
          }
          return tr.persistent();
        }
      };
    final IFn fn;
    if (coll.repetition == Schema.LIST) {
      if (coll.fn == null) {
        fn = asListFn;
      } else {
        fn = Utils.comp(asListFn, coll.fn);
      }
    } else {
      fn = coll.fn;
    }
    if (fn == null) {
      return repeatedFn;
    } else {
      return new Fn() {
        public Object invoke(ListIterator[] iterators) {
          return fn.invoke(repeatedFn.invoke(iterators));
        }
      };
    }
  }

  private static Fn getMapFn(Types types, Schema.Collection coll) {
    final Fn listFn = getFn(types, coll.withRepetition(Schema.LIST).withFn(null));
    final Fn mapFn = new Fn() {
        public Object invoke(ListIterator[] iterators) {
          Object list = listFn.invoke(iterators);
          if (list == null) {
            return null;
          } else {
            ISeq s = RT.seq(list);
            ITransientMap tm = PersistentArrayMap.EMPTY.asTransient();
            while (s != null) {
              Object e = s.first();
              tm = tm.assoc(RT.get(e, Schema.KEY), RT.get(e, Schema.VAL));
              s = s.next();
            }
            return tm.persistent();
          }
        }
      };
    final IFn fn = coll.fn;
    if (fn == null) {
      return mapFn;
    } else {
      return new Fn() {
        public Object invoke(ListIterator[] iterators) {
          return fn.invoke(mapFn.invoke(iterators));
        }
      };
    }
  }
}
