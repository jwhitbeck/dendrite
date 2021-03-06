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

public final class IntPackedRunLength {

  public static final class Decoder implements IIntDecoder {

    private final IntFixedBitWidthPackedRunLength.Decoder int32Decoder;

    public Decoder(ByteBuffer byteBuffer) {
      ByteBuffer bb = byteBuffer.slice();
      int width = (int)bb.get() & 0xff;
      int32Decoder = new IntFixedBitWidthPackedRunLength.Decoder(bb, width);
    }

    @Override
    public Object decode() {
      return int32Decoder.decode();
    }

    @Override
    public int decodeInt() {
      return int32Decoder.decodeInt();
    }

    @Override
    public int getNumEncodedValues() {
      return int32Decoder.getNumEncodedValues();
    }
  }


  public static final class Encoder extends AEncoder {

    private final MemoryOutputStream intBuffer;
    private final IntFixedBitWidthPackedRunLength.Encoder rleEncoder;
    private int numBufferedValues;
    private int maxWidth;
    private boolean isFinished;

    public Encoder() {
      rleEncoder = new IntFixedBitWidthPackedRunLength.Encoder(0);
      intBuffer = new MemoryOutputStream();
      maxWidth = 0;
      numBufferedValues = 0;
      isFinished = false;
    }

    @Override
    public void encode(Object o) {
      final int i = (int) o;
      final int width = Bytes.getBitWidth(i);
      if (width > maxWidth) {
        maxWidth = width;
      }
      Bytes.writeUInt(intBuffer, i);
      numBufferedValues += 1;
    }

    @Override
    public void reset() {
      rleEncoder.reset();
      intBuffer.reset();
      maxWidth = 0;
      numBufferedValues = 0;
      isFinished = false;
    }

    @Override
    public void finish() {
      if (!isFinished) {
        rleEncoder.setWidth(maxWidth);
        ByteBuffer intBufferReader = intBuffer.toByteBuffer();
        for (int j=0; j<numBufferedValues; ++j) {
          rleEncoder.encode(Bytes.readUInt(intBufferReader));
        }
        rleEncoder.finish();
        isFinished = true;
      }
    }

    @Override
    public int getLength() {
      return 1 + rleEncoder.getLength();
    }

    @Override
    public int getEstimatedLength() {
      int estimatedNumOctoplets = (numBufferedValues / 8) + 1;
      return 1 + Bytes.getNumUIntBytes(estimatedNumOctoplets << 1)
        + (estimatedNumOctoplets * maxWidth) + Bytes.getNumUIntBytes(numBufferedValues);
    }

    @Override
    public void writeTo(MemoryOutputStream memoryOutputStream) {
      finish();
      memoryOutputStream.write(maxWidth);
      memoryOutputStream.write(rleEncoder);
    }

    @Override
    public int getNumEncodedValues() {
      return numBufferedValues;
    }
  }

  public static final IDecoderFactory decoderFactory = new ADecoderFactory() {
      @Override
      public IDecoder create(ByteBuffer bb) {
        return new Decoder(bb);
      }
    };

}
