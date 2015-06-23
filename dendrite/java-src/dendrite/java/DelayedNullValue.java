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

public abstract class DelayedNullValue {

  public abstract Object get();

  private final static DelayedNullValue NULL = new AlwaysNull();

  public static DelayedNullValue withFn(IFn fn) {
    if (fn == null) {
      return NULL;
    }
    return new FunctionAppliedToNull(fn);
  }

  private static final class AlwaysNull extends DelayedNullValue {
    @Override
    public Object get() {
      return null;
    }
  }

  private static final class FunctionAppliedToNull extends DelayedNullValue {
    private final IFn fn;
    private boolean isNullValueSet = false;
    private Object nullValue;

    FunctionAppliedToNull(IFn fn) {
      this.fn = fn;
    }

    @Override
    public Object get() {
      if (!isNullValueSet) {
        nullValue = fn.invoke(null);
        isNullValueSet = true;
      }
      return nullValue;
    }
  }

}
