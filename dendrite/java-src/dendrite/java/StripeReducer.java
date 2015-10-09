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
import clojure.lang.IFn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class StripeReducer {

  private final int numColumns;
  private final int bundleSize;
  private final IFn reduceFn;
  private final IFn errorHandlerFn;


  public StripeReducer(Stripe.Fn stripeFn, int numColumns, int bundleSize, IFn xform, IFn errorHandlerFn) {
    this.numColumns = numColumns;
    this.bundleSize = bundleSize;
    IFn innerReduceFn = new InnerReduceFn(stripeFn);
    this.reduceFn = (xform == null)? innerReduceFn : (IFn)xform.invoke(innerReduceFn);
    this.errorHandlerFn = errorHandlerFn;
  }

  public InnerReturnValue init() {
    return new InnerReturnValue(numColumns, bundleSize);
  }

  public Bundle complete(InnerReturnValue ret) {
    return new Bundle(ret.n, ret.columnValues);
  }

  public InnerReturnValue step(InnerReturnValue ret, Object record) {
    try {
      return (InnerReturnValue)reduceFn.invoke(ret, record);
    } catch (Exception e) {
      if (errorHandlerFn != null) {
        errorHandlerFn.invoke(record, e);
        return ret;
      } else {
        throw new IllegalArgumentException(String.format("Failed to stripe record '%s'", record), e);
      }
    }
  }

  public Bundle reduce(List<Object> records) {
    InnerReturnValue ret = init();
    for (Object record : records) {
      ret = step(ret, record);
    }
    return complete(ret);
  }

  private static final class InnerReduceFn extends AFn {
    private final Stripe.Fn stripeFn;

    private InnerReduceFn(Stripe.Fn stripeFn) {
      this.stripeFn = stripeFn;
    }

    @Override
    public Object invoke(Object ret, Object record) {
      InnerReturnValue r = (InnerReturnValue)ret;
      r.clearBuffer();
      stripeFn.invoke(record, r.buffer);
      r.flushBuffer();
      r.n += 1;
      return r;
    }
  }

  @SuppressWarnings("unchecked")
  private static final class InnerReturnValue {
    private int n;
    private final Object[] buffer;
    private final List[] columnValues;

    InnerReturnValue(int numColumns, int bundleSize) {
      n = 0;
      buffer = new Object[numColumns];
      columnValues = new List[numColumns];
      for (int i=0; i<numColumns; ++i) {
        columnValues[i] = new ArrayList(bundleSize);
      }
    }

    void clearBuffer() {
      Arrays.fill(buffer, null);
    }

    void flushBuffer() {
      int numColumns = columnValues.length;
      for (int i=0; i<numColumns; ++i) {
        columnValues[i].add(buffer[i]);
      }
    }
  }

}
