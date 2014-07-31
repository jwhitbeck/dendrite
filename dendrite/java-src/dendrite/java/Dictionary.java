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
    for (Object o : dictDecoder) {
      a[i] = o;
      i += 1;
    }
    return a;
  }

  private static Object[] readFn(final Decoder dictDecoder, IFn fn) {
    Object[] a = new Object[dictDecoder.numEncodedValues()];
    int i = 0;
    for (Object o : dictDecoder) {
      a[i] = fn.invoke(o);
      i += 1;
    }
    return a;
  }

}
