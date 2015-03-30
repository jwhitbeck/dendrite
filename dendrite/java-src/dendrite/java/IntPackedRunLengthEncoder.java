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

public class IntPackedRunLengthEncoder extends AEncoder {

  private final MemoryOutputStream intBuffer;
  private final IntFixedBitWidthPackedRunLengthEncoder rleEncoder;
  private int numBufferedValues;
  private int maxWidth;
  private boolean isFinished;

  public IntPackedRunLengthEncoder() {
    rleEncoder = new IntFixedBitWidthPackedRunLengthEncoder(0);
    intBuffer = new MemoryOutputStream();
    maxWidth = 0;
    numBufferedValues = 0;
    isFinished = false;
  }

  @Override
  public void encode(final Object o) {
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
      ByteBuffer intBufferReader = intBuffer.byteBuffer();
      for (int j=0; j<numBufferedValues; ++j) {
        rleEncoder.encode(Bytes.readUInt(intBufferReader));
      }
      rleEncoder.finish();
      isFinished = true;
    }
  }

  @Override
  public int length() {
    return 1 + rleEncoder.length();
  }

  @Override
  public int estimatedLength() {
    int estimatedNumOctoplets = (numBufferedValues / 8) + 1;
    return 1 + Bytes.getNumUIntBytes(estimatedNumOctoplets << 1)
      + (8 * estimatedNumOctoplets * maxWidth) + Bytes.getNumUIntBytes(numBufferedValues);
  }

  @Override
  public void writeTo(final MemoryOutputStream memoryOutputStream) {
    finish();
    memoryOutputStream.write(maxWidth);
    rleEncoder.writeTo(memoryOutputStream);
  }

  @Override
  public int numEncodedValues() {
    return numBufferedValues;
  }

}
