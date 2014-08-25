/**
* Copyright (c) 2013-2014 John Whitbeck. All rights reserved.
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

import java.math.BigInteger;
import java.util.Arrays;

public class LongPackedDeltaEncoder extends AbstractEncoder {

  private static final int MAX_BLOCK_LENGTH = 128;
  private static final int MAX_MINIBLOCK_LENGTH = 32;
  private static final int MIN_MINIBLOCK_LENGTH = 8;

  private BigInteger[] valueBuffer = new BigInteger[MAX_BLOCK_LENGTH + 1];
  private int position = 0;
  private BigInteger[] deltas = new BigInteger[MAX_BLOCK_LENGTH];
  private BigInteger minDelta;
  private long [] referenceFrame = new long[MAX_BLOCK_LENGTH];
  private int numEncodedValues = 0;

  private ByteArrayWriter bestEncoding = new ByteArrayWriter(128);
  private ByteArrayWriter currentEncoding = new ByteArrayWriter(128);

  @Override
  public void encode(final Object o) {
    numValues += 1;
    if (position == MAX_BLOCK_LENGTH + 1) {
      flushBlock();
    }
    valueBuffer[position] = BigInteger.valueOf((long)o);
    position += 1;
  }

  private ByteArrayWriter getBestMiniblockEncodingForBlock() {
    int blockLength = getBlockLength(position);
    computeDeltas(blockLength);
    computeFrameOfReference();

    int miniblockLength = MIN_MINIBLOCK_LENGTH;

    bestEncoding.reset();
    flushBlockWithNumMiniBlocks(bestEncoding, miniblockLength, blockLength);

    int minLength = bestEncoding.length();

    miniblockLength <<= 1;
    while (miniblockLength <= Math.min(MAX_BLOCK_LENGTH, blockLength)) {
      currentEncoding.reset();
      flushBlockWithNumMiniBlocks(currentEncoding, miniblockLength, blockLength);
      int currentLength = currentEncoding.length();
      if (currentLength < minLength) {
        ByteArrayWriter tmp = bestEncoding;
        bestEncoding = currentEncoding;
        currentEncoding = tmp;
        minLength = currentLength;
      }
      miniblockLength <<= 1;
    }

    return bestEncoding;
  }

  private void flushBlockWithNumMiniBlocks(final ByteArrayWriter baw, final int miniBlockLength,
                                           final int blockLength) {
    int numMiniblocks = blockLength / miniBlockLength;
    long startValue = valueBuffer[0].longValue();
    int[] miniblockBitWidths = new int[numMiniblocks];
    for (int j=0; j<numMiniblocks; ++j) {
      miniblockBitWidths[j] = getMiniBlockBitWidth(j * miniBlockLength, miniBlockLength);
    }
    baw.writeUInt(blockLength);
    baw.writeUInt(numMiniblocks);
    baw.writeUInt(position);
    baw.writeSLong(startValue);
    if (numMiniblocks > 0){
      baw.writeSIntVLQ(minDelta);
      for (int j=0; j<numMiniblocks; ++j) {
        baw.writeByte((byte) (miniblockBitWidths[j] & 0xff));
      }
      for (int j=0; j<numMiniblocks; ++j) {
        int numRemainingValues = position - 1 - j * miniBlockLength;
        int length = numRemainingValues < miniBlockLength ? numRemainingValues : miniBlockLength;
        baw.writePackedInts64(referenceFrame, miniblockBitWidths[j], j * miniBlockLength, length);
      }
    }
  }

  private int getMiniBlockBitWidth(final int miniBlockStartPosition, final int miniBlockLength) {
    int maxBitWidth = 0;
    for (int j=miniBlockStartPosition; j<miniBlockStartPosition+miniBlockLength; ++j) {
      int bitWidth = ByteArrayWriter.getBitWidth(referenceFrame[j]);
      if (bitWidth > maxBitWidth) {
        maxBitWidth = bitWidth;
      }
    }
    return maxBitWidth;
  }

  private static final BigInteger INFINITY = BigInteger.valueOf(Long.MAX_VALUE).shiftLeft(2);

  private void computeDeltas(final int blockLength) {
    minDelta = INFINITY;
    for (int j=0; j<position-1; ++j) {
      BigInteger delta = valueBuffer[j+1].subtract(valueBuffer[j]);
      deltas[j] = delta;
      if (delta.compareTo(minDelta) == -1) {
        minDelta = delta;
      }
    }
  }

  private void computeFrameOfReference() {
    Arrays.fill(referenceFrame, 0, position-1, 0);
    for (int j=0; j<position-1; ++j) {
      BigInteger relativeDelta = deltas[j].subtract(minDelta);
      referenceFrame[j] = relativeDelta.longValue();
    }
  }

  private static int getBlockLength(final int numValuesInBlock) {
    int intsAfterFirst = numValuesInBlock - 1;
    if (intsAfterFirst == 0) {
      return 0;
    } else {
      int blockLength = 8;
      while (intsAfterFirst > blockLength && blockLength < MAX_BLOCK_LENGTH) {
        blockLength <<= 1;
      }
      return blockLength;
    }
  }

  private void flushBlock() {
    if (position > 0) {
      ByteArrayWriter fullBlockEncoding = getBestMiniblockEncodingForBlock();
      fullBlockEncoding.flush(byteArrayWriter);
      numEncodedValues += position;
    }
    position = 0;
  }

  @Override
  public void reset() {
    position = 0;
    numEncodedValues = 0;
    super.reset();
  }

  @Override
  public void finish() {
    if (position > 0){
      flushBlock();
    }
  }

  @Override
  public int estimatedLength() {
    if (numEncodedValues == 0) {
      return position; // very rough estimate when we haven't flushed anything yet
    }
    return super.estimatedLength() * (int)(1 + (double)position / (double)numEncodedValues);
  }

}
