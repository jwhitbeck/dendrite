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

public final class BooleanPacked {

  public final static class Decoder extends ADecoder {

    private final boolean[] octuplet = new boolean[8];
    private int position = 0;

    public Decoder(ByteBuffer byteBuffer){
      super(byteBuffer);
    }

    @Override
    public Object decode() {
      if ((position % 8) == 0) {
        Bytes.readPackedBooleans(bb, octuplet);
        position = 0;
      }
      boolean b = octuplet[position];
      position += 1;
      return b;
    }
  }

  public static class Encoder extends AEncoder {

    private boolean[] octuplet = new boolean[8];
    private int position = 0;

    @Override
    public void encode(Object o) {
      final boolean b = (boolean) o;
      numValues += 1;
      octuplet[position] = b;
      position += 1;
      if (position == 8) {
        Bytes.writePackedBooleans(mos, octuplet);
        position = 0;
      }
    }

    @Override
    public void reset() {
      position = 0;
      super.reset();
    }

    @Override
    public void finish() {
      if (position > 0){
        Bytes.writePackedBooleans(mos, octuplet);
        position = 0;
      }
    }

    @Override
    public int estimatedLength() {
      return super.estimatedLength() + (position == 0? 0 : 1);
    }
  }

  public final static IDecoderFactory decoderFactory = new IDecoderFactory() {
      @Override
      public IDecoder create(ByteBuffer bb) {
        return new Decoder(bb);
      }
    };
}
