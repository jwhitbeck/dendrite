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

public final class ByteArrayDeltaLength {

  public final static class Decoder extends ADecoder {

    private final IntPackedDelta.Decoder lengthsDecoder;

    public Decoder(ByteBuffer byteBuffer) {
      super(byteBuffer);
      int lengthsNumBytes = Bytes.readUInt(bb);
      lengthsDecoder = new IntPackedDelta.Decoder(bb);
      bb.position(bb.position() + lengthsNumBytes);
    }

    @Override
    public Object decode() {
      int length = lengthsDecoder.decodeInt();
      byte[] byteArray = new byte[length];
      bb.get(byteArray, 0, length);
      return byteArray;
    }

    public void decodeInto(MemoryOutputStream mos) {
      int length = lengthsDecoder.decodeInt();
      mos.ensureRemainingCapacity(length);
      bb.get(mos.buffer, mos.position, length);
      mos.position += length;
    }
  }

  public final static class Encoder implements IEncoder {

    private final IntPackedDelta.Encoder lengthsEncoder;
    private final MemoryOutputStream byteArrayBuffer;
    private int numValues = 0;

    public Encoder() {
      lengthsEncoder = new IntPackedDelta.Encoder();
      byteArrayBuffer = new MemoryOutputStream();
    }

    @Override
    public void encode(Object o) {
      final byte[] bs = (byte[]) o;
      numValues += 1;
      lengthsEncoder.encode(bs.length);
      byteArrayBuffer.write(bs, 0, bs.length);
    }

    public void encode(byte[] bs, int offset, int length) {
      numValues += 1;
      lengthsEncoder.encode(length);
      byteArrayBuffer.write(bs, offset, length);
    }

    @Override
    public void reset() {
      numValues = 0;
      byteArrayBuffer.reset();
      lengthsEncoder.reset();
    }

    @Override
    public void finish() {
      lengthsEncoder.finish();
    }

    @Override
    public int length() {
      return Bytes.getNumUIntBytes(numValues)
        + Bytes.getNumUIntBytes(lengthsEncoder.length()) + lengthsEncoder.length()
        + byteArrayBuffer.length();
    }

    public int estimatedLength() {
      int estimatedLengthsEncoderLength = lengthsEncoder.estimatedLength();
      return Bytes.getNumUIntBytes(numValues) + byteArrayBuffer.length()
        + Bytes.getNumUIntBytes(estimatedLengthsEncoderLength) + estimatedLengthsEncoderLength;
    }

    @Override
    public void writeTo(MemoryOutputStream memoryOutputStream) {
      finish();
      Bytes.writeUInt(memoryOutputStream, numValues);
      Bytes.writeUInt(memoryOutputStream, lengthsEncoder.length());
      lengthsEncoder.writeTo(memoryOutputStream);
      byteArrayBuffer.writeTo(memoryOutputStream);
    }

    @Override
    public int numEncodedValues() {
      return numValues;
    }
  }

  public final static IDecoderFactory decoderFactory = new ADecoderFactory() {
      @Override
      public IDecoder create(ByteBuffer bb) {
        return new Decoder(bb);
      }
    };
}
