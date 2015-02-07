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

public class ByteArrayDeltaLengthEncoder implements Encoder {

  private final IntPackedDeltaEncoder lengthsEncoder;
  private final ByteArrayWriter byteArrayWriter;
  private int numValues = 0;

  public ByteArrayDeltaLengthEncoder() {
    lengthsEncoder = new IntPackedDeltaEncoder();
    byteArrayWriter = new ByteArrayWriter();
  }

  @Override
  public void encode(final Object o) {
    final byte[] bs = (byte[]) o;
    numValues += 1;
    lengthsEncoder.encode(bs.length);
    byteArrayWriter.writeByteArray(bs, 0, bs.length);
  }

  public void encode(final byte[] bs, final int offset, final int length) {
    numValues += 1;
    lengthsEncoder.encode(length);
    byteArrayWriter.writeByteArray(bs, offset, length);
  }

  @Override
  public void reset() {
    numValues = 0;
    byteArrayWriter.reset();
    lengthsEncoder.reset();
  }

  @Override
  public void finish() {
    lengthsEncoder.finish();
  }

  @Override
  public int length() {
    return ByteArrayWriter.getNumUIntBytes(numValues)
      + ByteArrayWriter.getNumUIntBytes(lengthsEncoder.length()) + lengthsEncoder.length()
      + byteArrayWriter.length();
  }

  public int estimatedLength() {
    int estimatedLengthsEncoderLength = lengthsEncoder.estimatedLength();
    return ByteArrayWriter.getNumUIntBytes(numValues) + byteArrayWriter.length()
      + ByteArrayWriter.getNumUIntBytes(estimatedLengthsEncoderLength) + estimatedLengthsEncoderLength;
  }

  @Override
  public void flush(final ByteArrayWriter baw) {
    finish();
    baw.writeUInt(numValues);
    baw.writeUInt(lengthsEncoder.length());
    lengthsEncoder.flush(baw);
    byteArrayWriter.flush(baw);
  }

  @Override
  public int numEncodedValues() {
    return numValues;
  }

}
