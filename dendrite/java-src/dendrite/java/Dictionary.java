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

public final class Dictionary {

  public static Object[] read(final Decoder dictDecoder, final IFn fn) {
    if (fn == null) {
      return read(dictDecoder);
    }
    return readFn(dictDecoder, fn);
  }

  private static Object[] read(final Decoder dictDecoder) {
    Object[] a = new Object[dictDecoder.numEncodedValues()];
    int i = 0;
    while (i < a.length) {
      a[i] = dictDecoder.decode();
      i += 1;
    }
    return a;
  }

  private static Object[] readFn(final Decoder dictDecoder, IFn fn) {
    Object[] a = new Object[dictDecoder.numEncodedValues()];
    int i = 0;
    while (i < a.length) {
      a[i] = fn.invoke(dictDecoder.decode());
      i += 1;
    }
    return a;
  }

}
