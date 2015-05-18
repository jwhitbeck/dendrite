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

public final class LongPlain {

  public final static class Decoder extends ADecoder {

    public Decoder(ByteBuffer byteBuffer) {
      super(byteBuffer);
    }

    @Override
    public Object decode() {
      return Bytes.readFixedLong(bb);
    }

  }

  public final static class Encoder extends AEncoder {

    @Override
    public void encode(Object o) {
      numValues += 1;
      Bytes.writeFixedLong(mos, (long) o);
    }

  }

  public final static IDecoderFactory decoderFactory = new ADecoderFactory() {
      @Override
      public IDecoder create(ByteBuffer bb) {
        return new Decoder(bb);
      }
    };

}
