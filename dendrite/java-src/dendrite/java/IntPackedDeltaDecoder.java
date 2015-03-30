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

public class IntPackedDeltaDecoder extends AIntDecoder {

  private int[] miniblockBuffer = new int[128];
  private int miniblockPosition = 0;
  private int miniblockLength = 0;
  private int currentMiniblockIndex = 0;
  private int numMiniblocks = 0;
  private int[] miniblockBitWidths = new int[16];
  private int remainingValuesInBlock = 0;
  private int blockLength = 0;
  private long blockMinDelta = 0;
  private long blockCurrentValue = 0;

  public IntPackedDeltaDecoder(final ByteBuffer byteBuffer) {
    super(byteBuffer);
  }

  @Override
  public int decodeInt() {
    if (remainingValuesInBlock > 0) {
      if (miniblockPosition == -1) { // read from first value
        miniblockPosition = 0;
      } else if (currentMiniblockIndex == -1) { // no miniblock loaded
        initNextMiniBlock();
        setCurrentValueFromMiniBlockBuffer();
      } else if (miniblockPosition < miniblockLength) { // reading from buffered miniblock
        setCurrentValueFromMiniBlockBuffer();
      } else { // finished reading current mini block
        initNextMiniBlock();
        setCurrentValueFromMiniBlockBuffer();
      }
      remainingValuesInBlock -= 1;
      return (int)blockCurrentValue;
    } else { // no more values in block, load next block
      initNextBlock();
      return decodeInt();
    }
  }

  private void setCurrentValueFromMiniBlockBuffer() {
    long nextRelativeDelta = miniblockBuffer[miniblockPosition] & 0xffffffffL;
    long delta = nextRelativeDelta + blockMinDelta;
    blockCurrentValue += delta;
    miniblockPosition += 1;
  }

  private void initNextMiniBlock() {
    currentMiniblockIndex += 1;
    int width = miniblockBitWidths[currentMiniblockIndex];
    int length = remainingValuesInBlock < miniblockLength? remainingValuesInBlock : miniblockLength;
    Bytes.readPackedInts32(bb, miniblockBuffer, width, length);
    miniblockPosition = 0;
  }

  private void initNextBlock() {
    blockLength = Bytes.readUInt(bb);
    numMiniblocks = Bytes.readUInt(bb);
    miniblockLength = numMiniblocks > 0? blockLength / numMiniblocks : 0;
    remainingValuesInBlock = Bytes.readUInt(bb);
    miniblockPosition = -1;
    currentMiniblockIndex = -1;
    blockCurrentValue = Bytes.readSInt(bb);
    if (numMiniblocks > 0) {
      blockMinDelta = Bytes.readSLong(bb);
      for (int i=0; i<numMiniblocks; ++i) {
        miniblockBitWidths[i] = (int)bb.get() & 0xff;
      }
    }
  }

}
