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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public final class Dictionary {

  // Dictionary values are encoded as ints
  private final static int MAX_DISTINCT_VALUES = Integer.MAX_VALUE - 1;

  public final static class Decoder implements IDecoder {

    final IIntDecoder indicesDecoder;
    final Object[] dictionary;

    public Decoder(IIntDecoder indicesDecoder, Object[] dictionary) {
      this.dictionary = dictionary;
      this.indicesDecoder = indicesDecoder;
    }

    @Override
    public Object decode() {
      return dictionary[indicesDecoder.decodeInt()];
    }

    @Override
    public int numEncodedValues() {
      return indicesDecoder.numEncodedValues();
    }

  }

  private final static class DictionaryIndex {
    final int idx;
    int cnt;

    DictionaryIndex(int idx) {
      this.idx = idx;
      this.cnt = 1;
    }
  }

  public static class Encoder implements IEncoder {

    final HashMap<Object, DictionaryIndex> dictionaryIndex;
    final IEncoder indicesEncoder;

    Encoder() {
      indicesEncoder = new IntPackedRunLength.Encoder();
      this.dictionaryIndex = new HashMap<Object, DictionaryIndex>();
    }

    @Override
    public void encode(Object o) {
      int idx = getIndex(o);
      indicesEncoder.encode(idx);
    }

    @Override
    public int numEncodedValues() {
      return indicesEncoder.numEncodedValues();
    }

    @Override
    public void finish() {
      indicesEncoder.finish();
    }

    @Override
    public void reset() {
      indicesEncoder.reset();
    }

    @Override
    public int estimatedLength() {
      return indicesEncoder.estimatedLength();
    }

    @Override
    public int length() {
      return indicesEncoder.length();
    }

    @Override
    public void writeTo(MemoryOutputStream mos) {
      mos.write(indicesEncoder);
    }

    public void resetDictionary() {
      dictionaryIndex.clear();
    }

    public int numDictionaryValues() {
      return dictionaryIndex.size();
    }

    public Object[] getDictionary() {
      Object[] dict = new Object[dictionaryIndex.size()];
      for (Map.Entry<Object,DictionaryIndex> e : dictionaryIndex.entrySet()) {
        dict[e.getValue().idx] = unwrap(e.getKey());
      }
      return dict;
    }

    public int[] getIndicesByFrequency() {
      DictionaryIndex[] indices = dictionaryIndex.values().toArray(new DictionaryIndex[]{});
      Arrays.sort(indices, new Comparator<DictionaryIndex>() {
          public int compare(DictionaryIndex dia, DictionaryIndex dib) {
            if (dia.cnt > dib.cnt) {
              return -1; // sort by descending cnt
            } else if (dia.cnt < dib.cnt) {
              return 1;
            } else {
              return 0;
            }
          }
        });
      int[] indicesByFrequency = new int[indices.length];
      for (int i=0; i<indices.length; ++i) {
        indicesByFrequency[i] = indices[i].idx;
      }
      return indicesByFrequency;
    }

    Object wrap(Object o) { return o; }

    Object unwrap(Object o) { return o; }

    int getIndex(Object o) {
      Object key = wrap(o);
      DictionaryIndex di = dictionaryIndex.get(key);
      if (di == null) {
        int idx = dictionaryIndex.size();
        if (idx > MAX_DISTINCT_VALUES) {
          throw new IllegalStateException(String.format("Dictionary size exceed allowed maximum (%d)",
                                                        MAX_DISTINCT_VALUES));
        }
        dictionaryIndex.put(key, new DictionaryIndex(idx));
        return idx;
      } else {
        di.cnt += 1;
        return di.idx;
      }
    }

    public static Encoder create(int type, IFn fn) {
      if (type == Types.BYTE_ARRAY) {
        if (fn == null) {
          return new ByteArrayEncoder();
        } else {
          return new ByteArrayEncoderWithFn(fn);
        }
      } else if (fn == null) {
        return new Encoder();
      } else /* if (fn != null) */ {
        return new EncoderWithFn(fn);
      }
    }
  }

  private final static class EncoderWithFn extends Encoder {
    final IFn fn;

    EncoderWithFn(IFn fn) {
      this.fn = fn;
    }

    @Override
    public void encode(Object o) {
      int idx = getIndex(fn.invoke(o));
      indicesEncoder.encode(idx);
    }
  }

  private final static class HashableByteArray {
    final byte[] array;

    HashableByteArray(byte[] array) {
      this.array = array;
    }

    @Override
    public int hashCode() {
      int hash = 1;
      for (int i=0; i<array.length; ++i) {
        hash = 31 * hash + (int)array[i];
      }
      return hash;
    }

    @Override
    public boolean equals(Object o) {
      return Arrays.equals(array, ((HashableByteArray)o).array);
    }
  }

  private final static class ByteArrayEncoder extends Encoder {
    @Override
    Object wrap(Object o) {
      return new HashableByteArray((byte[])o);
    }

    @Override
    Object unwrap(Object o) {
      return ((HashableByteArray)o).array;
    }
  }


  private final static class ByteArrayEncoderWithFn extends Encoder {
    final IFn fn;

    ByteArrayEncoderWithFn(IFn fn) {
      this.fn = fn;
    }

    @Override
    public void encode(Object o) {
      int idx = getIndex(fn.invoke(o));
      indicesEncoder.encode(idx);
    }

    @Override
    Object wrap(Object o) {
      return new HashableByteArray((byte[])o);
    }

    @Override
    Object unwrap(Object o) {
      return ((HashableByteArray)o).array;
    }
  }

  public final static class DecoderFactory implements IDecoderFactory {
    final Object[] dictionary;
    final IDecoderFactory intDecoderFactory;

    public DecoderFactory(Object[] dictionary, IDecoderFactory intDecoderFactory) {
      this.dictionary = dictionary;
      this.intDecoderFactory = intDecoderFactory;
    }

    @Override
    public IDecoder create(ByteBuffer bb) {
      return new Decoder((IIntDecoder)intDecoderFactory.create(bb), dictionary);
    }
  }
}
