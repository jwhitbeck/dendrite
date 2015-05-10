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
import clojure.lang.ASeq;
import clojure.lang.IFn;
import clojure.lang.IPersistentCollection;
import clojure.lang.IPersistentMap;
import clojure.lang.ISeq;
import clojure.lang.RT;

public final class Utils {

  public static IFn comp(final IFn f, final IFn g) {
    return new AFn() {
      public Object invoke(Object o) {
        return f.invoke(g.invoke(o));
      }
    };
  }

  public static IFn comp(final IFn f, final IFn g, final IFn h) {
    return new AFn() {
      public Object invoke(Object o) {
        return f.invoke(g.invoke(h.invoke(o)));
      }
    };
  }

  public static IFn comp(final IFn... fs) {
    if (fs.length == 0) {
      return null;
    } else if (fs.length == 1) {
      return fs[0];
    } else if (fs.length == 2) {
      return comp(fs[0], fs[1]);
    } else if (fs.length == 3) {
      return comp(fs[0], fs[1], fs[2]);
    } else {
      return new AFn() {
        public Object invoke(Object o) {
          Object ret = o;
          for (int i=0; i<fs.length; ++i) {
            ret = fs[i].invoke(ret);
          }
          return ret;
        }
      };
    }
  }

  private final static class Concat extends ASeq {

    private final ISeq head;
    private final ISeq tail;

    private Concat(ISeq head, ISeq tail) {
      this.head = head;
      this.tail = tail;
    }

    private Concat(IPersistentMap meta, ISeq head, ISeq tail) {
      super(meta);
      this.head = head;
      this.tail = tail;
    }

    @Override
    public Concat withMeta(IPersistentMap meta){
      return new Concat(meta, head, tail);
    }

    @Override
    public int count() {
      return RT.count(head) + RT.count(tail);
    }

    @Override
    public Object first() {
      return head.first();
    }

    @Override
    public ISeq next() {
      ISeq next = head.next();
      if (next == null) {
        return RT.seq(tail);
      }
      return new Concat(next, tail);
    }
  }

  public static ISeq concat(Object head, Object tail) {
    if (RT.seq(head) == null) {
      return RT.seq(tail);
    } else if (RT.seq(tail) == null) {
      return RT.seq(head);
    }
    return new Concat(RT.seq(head), RT.seq(tail));
  }

}
