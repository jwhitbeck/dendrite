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

import clojure.lang.IFn;
import clojure.lang.ISeq;
import clojure.lang.IPersistentCollection;
import clojure.lang.ITransientCollection;

public final class StripedRecordBundle {

  private final ISeq[] columnValueSeq;
  private final Object[] leveledValuesArray;
  private final int numRecords;

  public StripedRecordBundle(final ISeq[] columnValueSeq, final int numRecords) {
    this.columnValueSeq = columnValueSeq;
    this.leveledValuesArray = new Object[columnValueSeq.length];
    this.numRecords = numRecords;
  }

  private Object getNextRecord(final IFn assemblyFn) {
    for (int j=0; j < columnValueSeq.length; ++j) {
      leveledValuesArray[j] = columnValueSeq[j].first();
      columnValueSeq[j] = columnValueSeq[j].next();
    }
    return assemblyFn.invoke(leveledValuesArray);
  }

  public IPersistentCollection assemble(final IFn assemblyFn) {
    ITransientCollection records = PersistentLinkedSeq.newEmptyTransient();
    for (int i=0; i<numRecords; ++i) {
      records = records.conj(getNextRecord(assemblyFn));
    }
    return records.persistent();
  }

  public Object reduce(final IFn reduceFn, final IFn assemblyFn, final Object init) {
    Object ret = init;
    for (int i=0; i<numRecords; ++i) {
      ret = reduceFn.invoke(ret, getNextRecord(assemblyFn));
    }
    return ret;
  }

  public Object reduce(final IFn reduceFn, final IFn assemblyFn) {
    Object ret = getNextRecord(assemblyFn);
    for (int i=1; i<numRecords; ++i) {
      ret = reduceFn.invoke(ret, getNextRecord(assemblyFn));
    }
    return ret;
  }
}
