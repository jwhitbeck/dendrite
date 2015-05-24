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

public final class WriteBundle implements IPersistentCollection, Counted, Sequential {

  public final ChunkedPersistentList[] columnValues;

  public WriteBundle(final ChunkedPersistentList[] columnValues) {
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
    return new WriteBundle(new ChunkedPersistentList[]{});
  }

  @Override
  public int count() {
    return RT.count(columnValues[0]);
  }

  public WriteBundle take(int n) {
    ChunkedPersistentList[] takenColumnValues = new ChunkedPersistentList[columnValues.length];
    for (int i=0; i<columnValues.length; ++i) {
      takenColumnValues[i] = columnValues[i].take(n);
    }
    return new WriteBundle(takenColumnValues);
  }

  public WriteBundle drop(int n) {
    ChunkedPersistentList[] droppedColumnValues = new ChunkedPersistentList[columnValues.length];
    for (int i=0; i<columnValues.length; ++i) {
      droppedColumnValues[i] = columnValues[i].drop(n);
    }
    return new WriteBundle(droppedColumnValues);
  }

  public static WriteBundle stripe(Object records, Stripe.Fn stripeFn, int numColumns) {
    Object[] buffer = new Object[numColumns];
    ITransientCollection[] transientColumnValues = new ITransientCollection[numColumns];
    for(int i=0; i<numColumns; ++i) {
      transientColumnValues[i] = ChunkedPersistentList.EMPTY.asTransient();
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
    return new WriteBundle(columnValues);
  }

  @Override
  public ISeq seq() {
    return ArraySeq.create((Object[])columnValues);
  }
}