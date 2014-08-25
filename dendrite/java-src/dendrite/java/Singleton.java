/**
* Copyright (c) 2013-2014 John Whitbeck. All rights reserved.
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
import clojure.lang.IHashEq;
import clojure.lang.IPersistentCollection;
import clojure.lang.ISeq;
import clojure.lang.PersistentList;
import clojure.lang.RT;
import clojure.lang.Sequential;
import clojure.lang.Util;

import java.util.List;
import java.io.Serializable;

public final class Singleton implements ISeq, Sequential, Serializable, IHashEq {

  private final Object obj;

  public Singleton(final Object o) {
    obj = o;
  }

  @Override
  public Object first() {
    return obj;
  }

  @Override
  public ISeq next() {
    return null;
  }

  @Override
  public ISeq more() {
    return PersistentList.EMPTY;
  }

  @Override
  public ISeq cons(final Object o) {
    return new Cons(o, this);
  }

  @Override
  public int hasheq() {
    return obj.hashCode();
  }

  @Override
  public boolean equals(final Object o) {
      if(this == o) return true;
      if(!(o instanceof Sequential || o instanceof List))
        return false;
      ISeq as = RT.seq(o);
      if (as != null && Util.equals(obj,as.first()) && as.next() == null) {
        return true;
      }
      return false;
  }

  @Override
  public boolean equiv(final Object o) {
    return equals(o);
  }

  @Override
  public IPersistentCollection empty(){
    return PersistentList.EMPTY;
  }

  @Override
  public int count() {
    return 1;
  }

  @Override
  public ISeq seq() {
    return this;
  }
}
