package dendrite.java;

import java.util.Arrays;

public class IntPackedDeltaEncoder extends AbstractEncoder {

  private static final int MAX_BLOCK_LENGTH = 128;
  private static final int MAX_MINIBLOCK_LENGTH = 32;
  private static final int MIN_MINIBLOCK_LENGTH = 8;

  private long[] valueBuffer = new long[MAX_BLOCK_LENGTH + 1];
  private int position = 0;
  private long[] deltas = new long[MAX_BLOCK_LENGTH];
  private long minDelta = 0;
  private int [] referenceFrame = new int[MAX_BLOCK_LENGTH];
  private int numEncodedValues = 0;

  private ByteArrayWriter bestEncoding = new ByteArrayWriter(128);
  private ByteArrayWriter currentEncoding = new ByteArrayWriter(128);

  @Override
  public void encode(final Object o) {
    numValues += 1;
    if (position == MAX_BLOCK_LENGTH + 1) {
      flushBlock();
    }
    valueBuffer[position] = (int)o;
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

  private void flushBlockWithNumMiniBlocks(final ByteArrayWriter baw, final int miniblockLength,
                                           final int blockLength) {
    int numMiniblocks = blockLength / miniblockLength;
    int startValue = (int)valueBuffer[0];
    int[] miniblockBitWidths = new int[numMiniblocks];
    for (int j=0; j<numMiniblocks; ++j) {
      miniblockBitWidths[j] = getMiniBlockBitWidth(j * miniblockLength, miniblockLength);
    }
    baw.writeUInt(blockLength);
    baw.writeUInt(numMiniblocks);
    baw.writeUInt(position);
    baw.writeSInt(startValue);
    if (numMiniblocks > 0){
      baw.writeSLong(minDelta);
      for (int j=0; j<numMiniblocks; ++j) {
        baw.writeByte((byte) (miniblockBitWidths[j] & 0xff));
      }
      for (int j=0; j<numMiniblocks; ++j) {
        int numRemainingValues = position - 1 - j * miniblockLength;
        int length = numRemainingValues < miniblockLength ? numRemainingValues : miniblockLength;
        baw.writePackedInts32(referenceFrame, miniblockBitWidths[j], j * miniblockLength, length);
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

  private void computeDeltas(final int blockLength) {
    minDelta = Long.MAX_VALUE;
    for (int j=0; j<position-1; ++j) {
      long delta = valueBuffer[j+1] - valueBuffer[j];
      deltas[j] = delta;
      if (delta < minDelta) {
        minDelta = delta;
      }
    }
  }

  private void computeFrameOfReference() {
    Arrays.fill(referenceFrame, 0, position-1, 0);
    for (int j=0; j<position-1; ++j) {
      long relativeDelta = deltas[j] - minDelta;
      referenceFrame[j] = (int) relativeDelta;
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
