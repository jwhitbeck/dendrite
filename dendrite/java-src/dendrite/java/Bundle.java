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

public final class Bundle implements Seqable {

  private final ChunkedPersistentList[] columnValues;

  public Bundle(final ChunkedPersistentList[] columnValues) {
    this.columnValues = columnValues;
  }

  public Bundle take(int n) {
    ChunkedPersistentList[] takenColumnValues = new ChunkedPersistentList[columnValues.length];
    for (int i=0; i<columnValues.length; ++i) {
      takenColumnValues[i] = columnValues[i].take(n);
    }
    return new Bundle(takenColumnValues);
  }

  public Bundle drop(int n) {
    ChunkedPersistentList[] droppedColumnValues = new ChunkedPersistentList[columnValues.length];
    for (int i=0; i<columnValues.length; ++i) {
      droppedColumnValues[i] = columnValues[i].drop(n);
    }
    return new Bundle(droppedColumnValues);
  }

  private static Object getNextRecord(final Object[] buffer, final ChunkedPersistentList[] columnValues,
                                      final IFn assemblyFn) {
    for (int i=0; i < columnValues.length; ++i) {
      buffer[i] = columnValues[i].first();
      columnValues[i] = columnValues[i].next();
    }
    return assemblyFn.invoke(buffer);
  }

  public IPersistentCollection assemble(final IFn assemblyFn) {
    ITransientCollection records = ChunkedPersistentList.newEmptyTransient();
    Object[] buffer = new Object[columnValues.length];
    ChunkedPersistentList[] remainingColumnValues = new ChunkedPersistentList[columnValues.length];
    System.arraycopy(columnValues, 0, remainingColumnValues, 0, columnValues.length);
    int numRecords = columnValues[0].count();
    for (int i=0; i<numRecords; ++i) {
      records.conj(getNextRecord(buffer, remainingColumnValues, assemblyFn));
    }
    return records.persistent();
  }

  public Object reduce(final IFn reduceFn, final IFn assemblyFn, final Object init) {
    Object ret = init;
    Object[] buffer = new Object[columnValues.length];
    ChunkedPersistentList[] remainingColumnValues = new ChunkedPersistentList[columnValues.length];
    System.arraycopy(columnValues, 0, remainingColumnValues, 0, columnValues.length);
    int numRecords = columnValues[0].count();
    for (int i=0; i<numRecords; ++i) {
      ret = reduceFn.invoke(ret, getNextRecord(buffer, remainingColumnValues, assemblyFn));
    }
    return ret;
  }

  public Object reduce(final IFn reduceFn, final IFn assemblyFn) {
    Object[] buffer = new Object[columnValues.length];
    ChunkedPersistentList[] remainingColumnValues = new ChunkedPersistentList[columnValues.length];
    System.arraycopy(columnValues, 0, remainingColumnValues, 0, columnValues.length);
    int numRecords = columnValues[0].count();
    Object ret = getNextRecord(buffer, remainingColumnValues, assemblyFn);
    for (int i=1; i<numRecords; ++i) {
      ret = reduceFn.invoke(ret, getNextRecord(buffer, remainingColumnValues, assemblyFn));
    }
    return ret;
  }

  public static Bundle stripe(final IPersistentCollection records, final IFn stripeFn, final int numColumns) {
    Object[] buffer = new Object[numColumns];
    ITransientCollection[] transientColumnValues = new ITransientCollection[numColumns];
    for(int i=0; i<numColumns; ++i) {
      transientColumnValues[i] = ChunkedPersistentList.newEmptyTransient();
    }
    boolean success = false;
    for (ISeq s = RT.seq(records); s != null; s = s.next()) {
      Arrays.fill(buffer, null);
      success = (boolean) stripeFn.invoke(s.first(), buffer);
      if (success) { // striping succeeded
        for(int i=0; i<numColumns; ++i) {
          transientColumnValues[i].conj(buffer[i]);
        }
      }
    }
    ChunkedPersistentList[] columnValues = new ChunkedPersistentList[numColumns];
    for(int i=0; i<numColumns; ++i) {
      columnValues[i] = (ChunkedPersistentList)transientColumnValues[i].persistent();
    }
    return new Bundle(columnValues);
  }

  @Override
  public ISeq seq() {
    return ArraySeq.create((Object[])columnValues);
  }
}
