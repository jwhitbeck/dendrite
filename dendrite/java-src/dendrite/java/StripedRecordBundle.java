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

import clojure.lang.ArraySeq;
import clojure.lang.IFn;
import clojure.lang.ISeq;
import clojure.lang.IPersistentCollection;
import clojure.lang.ITransientCollection;
import clojure.lang.RT;
import clojure.lang.Seqable;

import java.util.Arrays;

public final class StripedRecordBundle implements Seqable {

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

  public static StripedRecordBundle stripe(final IPersistentCollection records, final IFn stripeFn,
                                           final int numColumns) {
    Object[] buffer = new Object[numColumns];
    ITransientCollection[] transientColumnValueSeq = new ITransientCollection[numColumns];
    ISeq[] columnValueSeq = new ISeq[numColumns];
    for(int i=0; i<numColumns; ++i) {
      transientColumnValueSeq[i] = PersistentLinkedSeq.newEmptyTransient();
    }
    int numRecords = 0;
    Object record = null;
    boolean success = false;
    ISeq recordSeq = RT.seq(records);
    while (recordSeq != null ){
      record = recordSeq.first();
      Arrays.fill(buffer, null);
      success = (boolean) stripeFn.invoke(record, buffer);
      if (success) { // striping succeeded
        numRecords += 1;
        for(int i=0; i<numColumns; ++i) {
          transientColumnValueSeq[i] = transientColumnValueSeq[i].conj(buffer[i]);
        }
      }
      recordSeq = recordSeq.next();
    }
    for(int i=0; i<numColumns; ++i) {
      columnValueSeq[i] = RT.seq(transientColumnValueSeq[i].persistent());
    }
    return new StripedRecordBundle(columnValueSeq, numRecords);
  }

  @Override
  public ISeq seq() {
    return ArraySeq.create((Object[])columnValueSeq);
  }
}
