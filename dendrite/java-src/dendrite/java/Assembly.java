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
import clojure.lang.ITransientCollection;;
import clojure.lang.RT;

public final class Assembly {

  public static IFn getNonRepeatedValueFn(final int colIdx) {
    return new AFn() {
      @Override
      public Object invoke(final Object leveledValuesArray) {
        Object[] lva = (Object[]) leveledValuesArray;
        ISeq leveledValues = RT.seq(lva[colIdx]);
        LeveledValue lv = (LeveledValue) leveledValues.first();
        lva[colIdx] = leveledValues.next();
        if (lv == null) {
          return null;
        }
        return lv.value;
      }
    };
  }

  public static IFn getRequiredNonRepeatedValueFn(final int colIdx) {
    return new AFn() {
      @Override
      public Object invoke(final Object leveledValuesArray) {
        return ((Object[]) leveledValuesArray)[colIdx];
      }
    };
  }

  public static IFn getRepeatedValueFn(final int colIdx, final int maxRepetitionLevel,
                                       final int maxDefinitionLevel, final IEditableCollection coll) {
    return new AFn() {
      @Override
      public Object invoke(final Object leveledValuesArray) {
        Object[] lva = (Object[]) leveledValuesArray;
        ISeq leveledValues = RT.seq(lva[colIdx]);
        LeveledValue firstLv = (LeveledValue) leveledValues.first();
        LeveledValue nextLv = (LeveledValue) RT.second(leveledValues);
        int nextRepetitionLevel = (nextLv == null)? 0 : nextLv.repetitionLevel;
        if ((firstLv.value == null)
            && (maxDefinitionLevel > firstLv.definitionLevel)
            && (maxRepetitionLevel > nextRepetitionLevel)) {
          lva[colIdx] = leveledValues.next();
          return null;
        } else {
          ITransientCollection tr = coll.asTransient();
          tr = tr.conj(firstLv.value);
          ISeq rlvs = leveledValues.next();
          Object ret = null;
          while (true) {
            LeveledValue lv = (LeveledValue) RT.first(rlvs);
            if ((lv != null) && (maxRepetitionLevel == lv.repetitionLevel)) {
              tr = tr.conj(lv.value);
              rlvs = rlvs.next();
            } else {
              lva[colIdx] = rlvs;
              ret = tr.persistent();
              break;
            }
          }
          return ret;
        }
      }
    };
  }

  public static IFn getRecordConstructorFn(final IPersistentCollection fieldNames,
                                           final IPersistentCollection fieldAssemblyFns) {
    final PersistentFixedKeysHashMap.Factory factory = PersistentFixedKeysHashMap.factory(fieldNames);
    final IFn[] fieldAssemblyFnArray = (IFn[]) RT.seqToTypedArray(IFn.class, RT.seq(fieldAssemblyFns));
    final int n = fieldAssemblyFnArray.length;
    return new AFn() {
      @Override
      public Object invoke(final Object leveledValuesArray) {
        Object[] lva = (Object[]) leveledValuesArray;
        Object[] vals = new Object[n];
        for (int i=0; i<n; ++i) {
          Object v = fieldAssemblyFnArray[i].invoke(lva);
          vals[i] = (v == null)? PersistentFixedKeysHashMap.UNDEFINED : v;
        }
        return factory.create(vals);
      }
    };
  }

  public static IFn getNonRepeatedRecordFn(final IFn recordConstructorFn) {
    return new AFn() {
      @Override
      public Object invoke(final Object leveledValuesArray) {
        Object[] lva = (Object[]) leveledValuesArray;
        IPersistentCollection record = (IPersistentCollection) recordConstructorFn.invoke(lva);
        if (record.count() == 0) {
          return null;
        }
        return record;
      }
    };
  }

  private static int getNextRepetitionLevel(final int colIdx, final Object[] leveledValues) {
    LeveledValue lv = (LeveledValue) RT.first(leveledValues[colIdx]);
    if (lv == null) {
      return 0;
    }
    return lv.repetitionLevel;
  }

  private static int getNextDefinitionLevel(final int colIdx, final Object[] leveledValues) {
    LeveledValue lv = (LeveledValue) RT.first(leveledValues[colIdx]);
    if (lv == null) {
      return 0;
    }
    return lv.definitionLevel;
  }

  public static IFn getRepeatedRecordFn(final IFn nonRepeatedAssemblyFn, final int colIdx,
                                        final int repetitionLevel, final int definitionLevel,
                                        final IEditableCollection coll) {
    return new AFn() {
      @Override
      public Object invoke(final Object leveledValuesArray) {
        Object[] lva = (Object[]) leveledValuesArray;
        int currentDefinitionLevel = getNextDefinitionLevel(colIdx, lva);
        Object firstRecord = nonRepeatedAssemblyFn.invoke(leveledValuesArray);
        if ((firstRecord == null) && (definitionLevel > currentDefinitionLevel)) {
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
      }
    };
  }
}
