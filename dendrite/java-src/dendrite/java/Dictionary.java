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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public final class Dictionary {

  // Dictionary values are encoded as ints
  private static final int MAX_DISTINCT_VALUES = Integer.MAX_VALUE - 1;

  public static final class Decoder implements IDecoder {

    private final IIntDecoder indicesDecoder;
    private final Object[] dictionary;

    public Decoder(IIntDecoder indicesDecoder, Object[] dictionary) {
      this.dictionary = dictionary;
      this.indicesDecoder = indicesDecoder;
    }

    @Override
    public Object decode() {
      return dictionary[indicesDecoder.decodeInt()];
    }

    @Override
    public int getNumEncodedValues() {
      return indicesDecoder.getNumEncodedValues();
    }

  }

  private static final class DictionaryIndex {
    private final int idx;
    private int cnt;

    DictionaryIndex(int idx) {
      this.idx = idx;
      this.cnt = 1;
    }
  }

  public static class Encoder implements IEncoder {

    private final HashMap<Object, DictionaryIndex> dictionaryIndex;
    private final IEncoder indicesEncoder;

    Encoder(IEncoder indicesEncoder) {
      this.indicesEncoder = indicesEncoder;
      this.dictionaryIndex = new HashMap<Object, DictionaryIndex>();
    }

    @Override
    public void encode(Object o) {
      int idx = getIndex(o);
      indicesEncoder.encode(idx);
    }

    @Override
    public int getNumEncodedValues() {
      return indicesEncoder.getNumEncodedValues();
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
    public int getEstimatedLength() {
      return indicesEncoder.getEstimatedLength();
    }

    @Override
    public int getLength() {
      return indicesEncoder.getLength();
    }

    @Override
    public void writeTo(MemoryOutputStream mos) {
      mos.write(indicesEncoder);
    }

    public void resetDictionary() {
      dictionaryIndex.clear();
    }

    public int getNumDictionaryValues() {
      return dictionaryIndex.size();
    }

    public Object[] getDictionary() {
      Object[] dict = new Object[dictionaryIndex.size()];
      for (Map.Entry<Object,DictionaryIndex> e : dictionaryIndex.entrySet()) {
        dict[e.getValue().idx] = unwrap(e.getKey());
      }
      return dict;
    }

    private static final Comparator<DictionaryIndex> mostFrequentFirst = new Comparator<DictionaryIndex>() {
      public int compare(DictionaryIndex dia, DictionaryIndex dib) {
            if (dia.cnt > dib.cnt) {
              return -1; // sort by descending cnt
            } else if (dia.cnt < dib.cnt) {
              return 1;
            } else {
              return 0;
            }
          }
    };

    public int[] getIndicesByFrequency() {
      DictionaryIndex[] indices = dictionaryIndex.values().toArray(new DictionaryIndex[]{});
      Arrays.sort(indices, mostFrequentFirst);
      int[] indicesByFrequency = new int[indices.length];
      for (int i=0; i<indices.length; ++i) {
        indicesByFrequency[indices[i].idx] = i;
      }
      return indicesByFrequency;
    }

    public Object[] getDictionaryByFrequency() {
      Object[] dictionary = getDictionary();
      int[] indicesByFrequency = getIndicesByFrequency();
      Object[] dictByFrequency = new Object[dictionaryIndex.size()];
      for (int i=0; i<dictionary.length; ++i) {
        dictByFrequency[indicesByFrequency[i]] = dictionary[i];
      }
      return dictByFrequency;
    }

    Object wrap(Object o) {
      return o;
    }

    Object unwrap(Object o) {
      return o;
    }

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

    public static Encoder create(int type, int indicesEncoding) {
      IEncoder indicesEncoder = Types.getPrimitiveEncoder(Types.INT, indicesEncoding);
      if (type == Types.BYTE_ARRAY) {
        return new ByteArrayEncoder(indicesEncoder);
      } else {
        return new Encoder(indicesEncoder);
      }
    }
  }

  private static final class ByteArrayEncoder extends Encoder {

    ByteArrayEncoder(IEncoder indicesEncoder) {
      super(indicesEncoder);
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

  public static final class DecoderFactory implements IDecoderFactory {
    private final Object[] dictionary;
    private final IDecoderFactory intDecoderFactory;
    private final IDecoderFactory dictDecoderFactory;

    public DecoderFactory(Object[] dictionary, IDecoderFactory intDecoderFactory,
                          IDecoderFactory dictDecoderFactory) {
      this.dictionary = dictionary;
      this.intDecoderFactory = intDecoderFactory;
      this.dictDecoderFactory = dictDecoderFactory;
    }

    @Override
    public IDecoder create(ByteBuffer bb) {
      return new Decoder((IIntDecoder)intDecoderFactory.create(bb), dictionary);
    }

    @Override
    public Object getNullValue() {
      return dictDecoderFactory.getNullValue();
    }
  }
}
