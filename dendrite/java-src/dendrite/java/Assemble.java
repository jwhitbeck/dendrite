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
import clojure.lang.Keyword;
import clojure.lang.IEditableCollection;
import clojure.lang.IFn;
import clojure.lang.IPersistentCollection;
import clojure.lang.ISeq;
import clojure.lang.ITransientCollection;
import clojure.lang.PersistentVector;
import clojure.lang.PersistentHashSet;
import clojure.lang.RT;

public final class Assemble {

  public static interface Fn {
    Object invoke(Object[] buffer);
  }

  public static Fn getFn(Schema schema) {
    if (schema instanceof Schema.Record) {
      return getRecordFn((Schema.Record)schema);
    } else if (schema instanceof Schema.Column) {
      if (schema.repetition < 0) {
        return getMissingValueFn((Schema.Column)schema);
      } else if (schema.repetitionLevel == 0) {
        return getNonRepeatedValueFn((Schema.Column)schema);
      } else {
        return getRepeatedValueFn((Schema.Column)schema);
      }
    } else /* if (schema instanceof Schema.Collection) */ {
      if (schema.repetition == Schema.MAP) {
        return getMapFn((Schema.Collection)schema);
      } else {
        return getRepeatedFn((Schema.Collection)schema);
      }
    }
  }

  static Fn getMissingValueFn(Schema.Column column) {
    IFn fn = column.fn;
    final Object v = (fn != null)? fn.invoke(null) : null;
    return new Fn() {
      public Object invoke(Object [] buffer) {
        return v;
      }
    };
  }

  static Fn getNonRepeatedValueFn(Schema.Column column) {
    final int colIdx = column.columnIndex;
    // NOTE: the column.fn is applied by the decoder for greater efficiency
    return new Fn() {
      public Object invoke(Object[] buffer) {
        return buffer[colIdx];
      }
    };
  }

  static Fn getRepeatedValueFn(Schema.Column column) {
    // NOTE: the column.fn is applied by the decoder for greater efficiency
    final int colIdx = column.columnIndex;
    return new Fn() {
      public Object invoke(Object[] buffer) {
        ISeq leveledValues = RT.seq(buffer[colIdx]);
        LeveledValue lv = (LeveledValue)leveledValues.first();
        buffer[colIdx] = leveledValues.next();
        return lv.value;
      }
    };
  }

  static interface RecordConstructorFn {
    public IPersistentCollection invoke(Object[] buffer);
  }

  static RecordConstructorFn getRecordConstructorFn(final Keyword[] fieldNames, final Fn[] fieldAssemblyFns) {
    final PersistentRecord.Factory factory = new PersistentRecord.Factory(fieldNames);
    final int n = fieldAssemblyFns.length;
    return new RecordConstructorFn() {
      public IPersistentCollection invoke(Object[] buffer) {
        Object[] vals = new Object[n];
        for (int i=0; i<n; ++i) {
          Object v = fieldAssemblyFns[i].invoke(buffer);
          vals[i] = (v == null)? PersistentRecord.UNDEFINED : v;
        }
        return factory.create(vals);
      }
    };
  }

  static Fn getRecordFn(Schema.Record record) {
    Schema.Field[] fields = record.fields;
    int n = fields.length;
    final Keyword[] fieldNames = new Keyword[n];
    final Fn[] fieldAssemblyFns = new Fn[n];
    for (int i=0; i<n; ++i) {
      Schema.Field field = fields[i];
      fieldNames[i] = field.name;
      fieldAssemblyFns[i] = getFn(field.value);
    }
    final RecordConstructorFn recordConstructorFn = getRecordConstructorFn(fieldNames, fieldAssemblyFns);
    if (record.fn == null) {
      return new Fn() {
        public Object invoke(Object[] buffer) {
          IPersistentCollection rec = recordConstructorFn.invoke(buffer);
          if (rec.count() == 0) {
            return null;
          } else {
            return rec;
          }
        }
      };
    } else {
      final IFn fn = record.fn;
      final Object emptyValue = fn.invoke(null);
      return new Fn() {
        public Object invoke(Object[] buffer) {
          IPersistentCollection rec = recordConstructorFn.invoke(buffer);
          if (rec.count() == 0) {
            return emptyValue;
          } else {
            return fn.invoke(rec);
          }
        }
      };
    }
  }

  static int getNextRepetitionLevel( Object[] buffer, int colIdx) {
    LeveledValue lv = (LeveledValue)RT.first(buffer[colIdx]);
    if (lv == null) {
      return 0;
    }
    return lv.repetitionLevel;
  }

  static int getNextDefinitionLevel( Object[] buffer, int colIdx) {
    LeveledValue lv = (LeveledValue)RT.first(buffer[colIdx]);
    if (lv == null) {
      return 0;
    }
    return lv.definitionLevel;
  }

  static Fn getRepeatedFn(Schema.Collection coll) {
    final int leafColumnIndex = coll.leafColumnIndex;
    final int repetitionLevel = coll.repetitionLevel;
    final int definitionLevel = coll.definitionLevel;
    final ITransientCollection emptyColl;
    switch (coll.repetition) {
    case Schema.SET: emptyColl = PersistentHashSet.EMPTY.asTransient(); break;
    case Schema.VECTOR: emptyColl = PersistentVector.EMPTY.asTransient(); break;
    default: /* Schema.LIST*/ emptyColl = ChunkedPersistentList.newEmptyTransient(); break;
    }
    final Fn repeatedElemFn = getFn(coll.repeatedSchema);
    return new Fn() {
      public Object invoke(Object[] buffer) {
        /* int currentDefinitionLevel = getNextDefinitionLevel(buffer, leafColumnIndex);
        Object firstObject = repeatedElemFn.invoke(buffer);
        if ((firstObject == null) && (definitionLevel > currentDefinitionLevel)) {
          return null;
        }
        IPersistentCollection ret = null;
        ITransientCollection tr = coll.asTransient();
        tr = tr.conj(firstRecord);
        int nextRepetitionLevel = getNextRepetitionLevel(colIdx, lva);
        while (true) {
          if (repetitionLevel > nextRepetitionLevel) {
            ret = tr.persistent();
            break;
          } else {
            Object nextRecord = nonRepeatedAssemblyFn.invoke(leveledValuesArray);
            tr = tr.conj(nextRecord);
            nextRepetitionLevel = getNextRepetitionLevel(colIdx, lva);
          }
        }
        if (ret.count() == 0) {
          return null;
        }
        return ret;
        }*/
        return null;
      }
    };
  }

  static Fn getMapFn(Schema.Collection coll) {
    return null;
  }
}
