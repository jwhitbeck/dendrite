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

public class IntFixedBitWidthPackedRunLengthDecoder extends AbstractIntDecoder {

  private final int[] octuplet = new int[8];
  private int octupletPosition = 8;
  private int numOctopletsToRead = 0;
  private int rleValue = 0;
  private int numRleValuesToRead = 0;
  private final int width;

  public IntFixedBitWidthPackedRunLengthDecoder(final ByteBuffer byteBuffer, final int width) {
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

  private void bufferNextRLERun(final int numOccurencesRleValue) {
    numRleValuesToRead = numOccurencesRleValue;
    rleValue = Bytes.readPackedInt(bb, width);
  }

  private void bufferNextPackedIntRun(final int numOctuplets) {
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
