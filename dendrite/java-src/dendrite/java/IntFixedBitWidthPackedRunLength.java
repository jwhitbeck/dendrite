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

public final class IntFixedBitWidthPackedRunLength {

  public final static class Decoder extends AIntDecoder {

    private final int[] octuplet = new int[8];
    private int octupletPosition = 8;
    private int numOctopletsToRead = 0;
    private int rleValue = 0;
    private int numRleValuesToRead = 0;
    private final int width;

    public Decoder(ByteBuffer byteBuffer, int width) {
      super(byteBuffer);
      this.width = width;
    }

    @Override
    public int decodeInt() {
      if (numRleValuesToRead > 0) {
        return decodeFromRLEValue();
      } else if (octupletPosition < 8) {
        return decodeFromOctuplet();
      } else {
        bufferNextRun();
        return decodeInt();
      }
    }

    private int decodeFromOctuplet() {
      int v = octuplet[octupletPosition];
      octupletPosition += 1;
      if (octupletPosition == 8 && numOctopletsToRead > 0) {
        bufferNextOctuplet();
      }
      return v;
    }

    private int decodeFromRLEValue() {
      numRleValuesToRead -= 1;
      return rleValue;
    }

    private void bufferNextOctuplet() {
      Bytes.readPackedInts32(bb, octuplet, width, 8);
      numOctopletsToRead -= 1;
      octupletPosition = 0;
    }

    private void bufferNextRLERun(int numOccurencesRleValue) {
      numRleValuesToRead = numOccurencesRleValue;
      rleValue = Bytes.readPackedInt(bb, width);
    }

    private void bufferNextPackedIntRun(int numOctuplets) {
      numOctopletsToRead = numOctuplets;
      bufferNextOctuplet();
    }

    private void bufferNextRun() {
      int n = Bytes.readUInt(bb);
      if ((n & 1) == 1) {
        bufferNextPackedIntRun(n >>> 1);
      } else {
        bufferNextRLERun(n >>> 1);
      }
    }

  }


  public final static class Encoder extends AEncoder {

    private int rleValue = 0;
    private int numOccurencesRleValue = 0;
    private int[] currentOctuplet = new int[8];
    private int currentOctupletPosition = 0;
    private MemoryOutputStream octupletBuffer = new MemoryOutputStream();
    private int numBufferedOctuplets;
    private int rleThreshold;
    private int width;

    public Encoder(final int width) {
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

}
