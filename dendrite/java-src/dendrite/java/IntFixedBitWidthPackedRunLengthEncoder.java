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

public class IntFixedBitWidthPackedRunLengthEncoder extends AEncoder {

  private int rleValue = 0;
  private int numOccurencesRleValue = 0;
  private int[] currentOctuplet = new int[8];
  private int currentOctupletPosition = 0;
  private MemoryOutputStream octupletBuffer = new MemoryOutputStream();
  private int numBufferedOctuplets;
  private int rleThreshold;
  private int width;

  public IntFixedBitWidthPackedRunLengthEncoder(final int width) {
    setWidth(width);
  }

  protected void setWidth(final int width) {
    this.width = width;
    rleThreshold = computeRLEThreshold(width);
  }

  private static int computeRLEThreshold(final int width) {
    int rleRunNumBytes = 2 + (width / 8);
    double numPackedValuesPerByte = (double)8 / (double)width;
    return (int)(rleRunNumBytes * numPackedValuesPerByte) + 1;
  }

  private void encodeInt(final int i) {
    if (currentOctupletPosition == 0) {
      if (numOccurencesRleValue == 0) {
        startRLERun(i);
      } else if (rleValue == i) {
        numOccurencesRleValue += 1;
      } else if (numOccurencesRleValue >= rleThreshold) {
        flushRLE();
        encodeInt(i);
      } else {
        packRLERun();
        encodeInt(i);
      }
    } else {
      bufferPackedInt(i);
    }
  }

  @Override
  public void encode(final Object o) {
    numValues += 1;
    encodeInt((int)o);
  }

  @Override
  public void reset() {
    super.reset();
    rleValue = 0;
    octupletBuffer.reset();
    currentOctupletPosition = 0;
    numOccurencesRleValue = 0;
    numBufferedOctuplets = 0;
  }

  @Override
  public void finish() {
    if (currentOctupletPosition > 0) {
      for (int j=currentOctupletPosition; j<8; j++) {
        currentOctuplet[j] = 0; // pad with zeros
      }
      currentOctupletPosition = 0;
      Bytes.writePackedInts32(octupletBuffer, currentOctuplet, width, 8);
      numBufferedOctuplets += 1;
      flushBitPacked();
    } else if (numOccurencesRleValue > 0) {
      flushRLE();
    } else if (numBufferedOctuplets > 0) {
      flushBitPacked();
    }
  }

  @Override
  public int estimatedLength() {
    if (numOccurencesRleValue > 0) {
      return super.estimatedLength() + Bytes.getNumUIntBytes(rleValue)
        + Bytes.getBitWidth(numOccurencesRleValue << 1);
    } else {
      return super.estimatedLength() + Bytes.getNumUIntBytes(numBufferedOctuplets << 1)
        + (8 * width * numBufferedOctuplets) + currentOctupletPosition > 0? (8 * width) : 0;
    }
  }

  private void packRLERun() {
    for (int j=0; j<numOccurencesRleValue; ++j) {
      bufferPackedInt(rleValue);
    }
    numOccurencesRleValue = 0;
  }

  private void startRLERun(final int i) {
    numOccurencesRleValue = 1;
    rleValue = i;
  }

  private void bufferPackedInt(final int i) {
    currentOctuplet[currentOctupletPosition] = i;
    currentOctupletPosition += 1;
    if (currentOctupletPosition == 8) {
      Bytes.writePackedInts32(octupletBuffer, currentOctuplet, width, 8);
      currentOctupletPosition = 0;
      numBufferedOctuplets += 1;
    }
  }

  private void flushBitPacked() {
    writeBitPackedHeader();
    flushBitPackedBuffer();
    numBufferedOctuplets = 0;
  }

  private void writeBitPackedHeader() {
    Bytes.writeUInt(mos, numBufferedOctuplets << 1 | 1);
  }

  private void flushBitPackedBuffer() {
    octupletBuffer.writeTo(mos);
    octupletBuffer.reset();
  }

  private void flushRLE() {
    if (numBufferedOctuplets > 0){
      flushBitPacked();
    }
    writeRLEHeader();
    writeRLEValue();
    numOccurencesRleValue = 0;
  }

  private void writeRLEHeader() {
    Bytes.writeUInt(mos, numOccurencesRleValue << 1);
  }

  private void writeRLEValue () {
    Bytes.writePackedInt(mos, rleValue, width);
  }

}
