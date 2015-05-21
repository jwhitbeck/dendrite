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

import clojure.lang.Cons;
import clojure.lang.IFn;
import clojure.lang.ISeq;
import clojure.lang.PersistentList;
import clojure.lang.Seqable;
import clojure.lang.Sequential;
import clojure.lang.RT;
import clojure.lang.Util;

public abstract class View implements ISeq, Seqable, Sequential {

  @Override
  public Object first() {
    return RT.first(seq());
  }

  @Override
  public ISeq next() {
    return RT.next(seq());
  }

  @Override
  public ISeq more() {
    ISeq next = next();
    if (next == null) {
      return PersistentList.EMPTY;
    }
    return next;
  }

  @Override
  public ISeq empty() {
    return PersistentList.EMPTY;
  }

  @Override
  public ISeq cons(Object o) {
    return new Cons(o, this);
  }

  @Override
  public int count() {
    return RT.count(seq());
  }

  @Override
  public boolean equiv(Object o) {
    return Util.equiv(seq(), o);
  }

  public abstract Object fold(int n, IFn combinef, IFn reducef);
}
