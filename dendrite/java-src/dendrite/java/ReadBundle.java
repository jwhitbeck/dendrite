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
import clojure.lang.Counted;
import clojure.lang.Cons;
import clojure.lang.IFn;
import clojure.lang.ISeq;
import clojure.lang.IPersistentCollection;
import clojure.lang.ITransientCollection;
import clojure.lang.RT;
import clojure.lang.Sequential;

import java.util.Arrays;

public final class ReadBundle implements IPersistentCollection, Counted, Sequential {

  public final ISeq[] columnValues;

  public ReadBundle(final ISeq[] columnValues) {
    this.columnValues = columnValues;
  }

  @Override
  public boolean equiv(Object o) {
    return RT.seq(this).equiv(RT.seq(o));
  }

  @Override
  public IPersistentCollection cons(Object o) {
    return new Cons(o, RT.seq(this));
  }

  @Override
  public IPersistentCollection empty() {
    return new ReadBundle(new ISeq[]{});
  }

  @Override
  public int count() {
    return RT.count(columnValues[0]);
  }

  private static Object getNextRecord(Object[] buffer, ISeq[] columnValues,
                                      Assemble.Fn assemblyFn) {
    for (int i=0; i < columnValues.length; ++i) {
      buffer[i] = columnValues[i].first();
      columnValues[i] = columnValues[i].next();
    }
    return assemblyFn.invoke(buffer);
  }

  public IPersistentCollection assemble(Assemble.Fn assemblyFn) {
    ITransientCollection records = ChunkedPersistentList.EMPTY.asTransient();
    Object[] buffer = new Object[columnValues.length];
    ISeq[] remainingColumnValues = new ISeq[columnValues.length];
    System.arraycopy(columnValues, 0, remainingColumnValues, 0, columnValues.length);
    int numRecords = columnValues[0].count();
    for (int i=0; i<numRecords; ++i) {
      records.conj(getNextRecord(buffer, remainingColumnValues, assemblyFn));
    }
    return records.persistent();
  }

  public Object reduce(IFn reduceFn, Assemble.Fn assemblyFn, Object init) {
    Object ret = init;
    Object[] buffer = new Object[columnValues.length];
    ISeq[] remainingColumnValues = new ISeq[columnValues.length];
    System.arraycopy(columnValues, 0, remainingColumnValues, 0, columnValues.length);
    int numRecords = columnValues[0].count();
    for (int i=0; i<numRecords; ++i) {
      ret = reduceFn.invoke(ret, getNextRecord(buffer, remainingColumnValues, assemblyFn));
    }
    return ret;
  }

  @Override
  public ISeq seq() {
    return ArraySeq.create((Object[])columnValues);
  }
}
