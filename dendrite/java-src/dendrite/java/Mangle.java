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
import clojure.lang.RT;

import java.util.List;

public final class Mangle {

  private static final Object REMOVED = new Object();

  public static boolean isFiltered(Object o) {
    return o != REMOVED;
  }

  public interface Fn {
    Object invoke(long index, Object o);
  }

  public static Fn getMapFn(final IFn f) {
    return new Fn() {
      public Object invoke(long index, Object o) {
        return f.invoke(o);
      }
    };
  }

  public static Fn getMapIndexedFn(final IFn f) {
    return new Fn() {
      public Object invoke(long index, Object o) {
        return f.invoke(index, o);
      }
    };
  }

  public static Fn getKeepFn(final IFn f) {
    return new Fn() {
      public Object invoke(long index, Object o) {
        Object ret = f.invoke(o);
        if (ret == null) {
          return REMOVED;
        } else {
          return ret;
        }
      }
    };
  }

  public static Fn getKeepIndexedFn(final IFn f) {
    return new Fn() {
      public Object invoke(long index, Object o) {
        Object ret = f.invoke(index, o);
        if (ret == null) {
          return REMOVED;
        } else {
          return ret;
        }
      }
    };
  }

  public static Fn getFilterFn(final IFn f) {
    return new Fn() {
      public Object invoke(long index, Object o) {
        if (RT.booleanCast(f.invoke(o))) {
          return o;
        } else {
          return REMOVED;
        }
      }
    };
  }

  public static Fn getFilterIndexedFn(final IFn f) {
    return new Fn() {
      public Object invoke(long index, Object o) {
        if (RT.booleanCast(f.invoke(index, o))) {
          return o;
        } else {
          return REMOVED;
        }
      }
    };
  }

  public static Fn compose(List<Fn> fns) {
    switch (fns.size()) {
    case 0: return null;
    case 1: return fns.get(0);
    case 2: return composeFns(fns.get(0), fns.get(1));
    case 3: return composeFns(fns.get(0), fns.get(1), fns.get(2));
    default: return composeFns(fns);
    }
  }

  private static Fn composeFns(final Fn f, final Fn g) {
    return new Fn() {
      public Object invoke(long index, Object o) {
        Object ret = f.invoke(index, o);
        if (ret != REMOVED) {
          ret = g.invoke(index, ret);
        }
        return ret;
      }
    };
  }

  private static Fn composeFns(final Fn f, final Fn g, final Fn h) {
    return new Fn() {
      public Object invoke(long index, Object o) {
        Object ret = f.invoke(index, o);
        if (ret != REMOVED) {
          ret = g.invoke(index, ret);
          if (ret != REMOVED) {
            ret = h.invoke(index, ret);
          }
        }
        return ret;
      }
    };
  }

  private static Fn composeFns(final List<Fn> fns) {
    return new Fn() {
      public Object invoke(long index, Object o) {
        Object ret = o;
        for (Fn f : fns) {
          if (ret == REMOVED) {
            break;
          }
          ret = f.invoke(index, ret);
        }
        return ret;
      }
    };
  }

}
