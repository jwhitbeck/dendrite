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

public final class ByteArrayIncremental {

  public final static class Decoder extends ADecoder {

    private final ByteArrayDeltaLength.Decoder byteArrayDecoder;
    private final IntPackedDelta.Decoder prefixLengthsDecoder;
    private final MemoryOutputStream buffer;

    public Decoder(ByteBuffer byteBuffer) {
      super(byteBuffer);
      int prefixLengthsNumBytes = Bytes.readUInt(bb);
      prefixLengthsDecoder = new IntPackedDelta.Decoder(bb);
      ByteBuffer byteArrayDeltaLengths = bb.slice();
      byteArrayDeltaLengths.position(byteArrayDeltaLengths.position() + prefixLengthsNumBytes);
      byteArrayDecoder = new ByteArrayDeltaLength.Decoder(byteArrayDeltaLengths);
      buffer = new MemoryOutputStream(128);
    }

    @Override
    public Object decode() {
      int prefixLength = prefixLengthsDecoder.decodeInt();
      buffer.position = prefixLength;
      byteArrayDecoder.decodeInto(buffer);
      byte[] byteArray = new byte[buffer.getLength()];
      System.arraycopy(buffer.buffer, 0, byteArray, 0, buffer.getLength());
      return byteArray;
    }

  }

  public final static class Encoder implements IEncoder {

    private byte[] previousByteArray = null;
    private final ByteArrayDeltaLength.Encoder byteArrayEncoder;
    private final IntPackedDelta.Encoder prefixLengthsEncoder;
    private int numValues = 0;

    public Encoder() {
      prefixLengthsEncoder = new IntPackedDelta.Encoder();
      byteArrayEncoder = new ByteArrayDeltaLength.Encoder();
    }

    @Override
    public void encode(Object o) {
      final byte[] bs = (byte[]) o;
      numValues += 1;
      int firstDifferentByteIdx = 0;
      if (previousByteArray != null) {
        int i = 0;
        while (i < Math.min(bs.length, previousByteArray.length)) {
          if (bs[i] != previousByteArray[i]) {
            break;
          }
          i++;
        }
        firstDifferentByteIdx = i;
      }
      prefixLengthsEncoder.encode(firstDifferentByteIdx);
      byteArrayEncoder.encode(bs, firstDifferentByteIdx, bs.length - firstDifferentByteIdx);
      previousByteArray = bs;
    }

    @Override
    public void reset() {
      numValues = 0;
      prefixLengthsEncoder.reset();
      byteArrayEncoder.reset();
      previousByteArray = null;
    }

    @Override
    public void finish() {
      prefixLengthsEncoder.finish();
      byteArrayEncoder.finish();
    }

    @Override
    public int getLength() {
      return Bytes.getNumUIntBytes(prefixLengthsEncoder.getLength())
        + Bytes.getNumUIntBytes(numValues)
        + prefixLengthsEncoder.getLength() + byteArrayEncoder.getLength();
    }

    @Override
    public int getEstimatedLength() {
      int estimatedPrefixLengthsEncoderLength = prefixLengthsEncoder.getEstimatedLength();
      return Bytes.getNumUIntBytes(estimatedPrefixLengthsEncoderLength)
        + Bytes.getNumUIntBytes(numValues)
        + estimatedPrefixLengthsEncoderLength + byteArrayEncoder.getLength();
    }

    @Override
    public void writeTo(MemoryOutputStream memoryOutputStream) {
      finish();
      Bytes.writeUInt(memoryOutputStream, numValues);
      Bytes.writeUInt(memoryOutputStream, prefixLengthsEncoder.getLength());
      prefixLengthsEncoder.writeTo(memoryOutputStream);
      byteArrayEncoder.writeTo(memoryOutputStream);
    }

    @Override
    public int getNumEncodedValues() {
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
