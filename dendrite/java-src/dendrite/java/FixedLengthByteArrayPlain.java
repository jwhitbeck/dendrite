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


public final class FixedLengthByteArrayPlain {

  public static final class Decoder extends ADecoder {

    private final int length;

    public Decoder(ByteBuffer byteBuffer) {
      super(byteBuffer);
      this.length = Bytes.readUInt(bb);
    }

    @Override
    public Object decode() {
      byte[] fixedLengthByteArray = new byte[length];
      bb.get(fixedLengthByteArray, 0, length);
      return fixedLengthByteArray;
    }

  }

  public static final class Encoder extends AEncoder {

    private int length = -1;

    @Override
    public void encode(Object o) {
      final byte[] bs = (byte[]) o;
      numValues += 1;
      if (length != bs.length) {
        if (length == -1) {
          length = bs.length;
          Bytes.writeUInt(mos, length);
        } else {
          throw new IllegalStateException("Different byte-array lengths in FixedLengthByteArrayEncoder");
        }
      }
      mos.write(bs, 0, length);
    }

    @Override
    public void reset() {
      super.reset();
      length = -1;
    }

  }

  public static final IDecoderFactory decoderFactory = new ADecoderFactory() {
      @Override
      public IDecoder create(ByteBuffer bb) {
        return new Decoder(bb);
      }
    };

}
