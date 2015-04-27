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
import java.util.Arrays;

public final class IntPackedDelta {

  public final static class Decoder extends AIntDecoder {

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

    public Decoder(ByteBuffer byteBuffer) {
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


  public final static class Encoder extends AEncoder {

    private static final int MAX_BLOCK_LENGTH = 128;
    private static final int MAX_MINIBLOCK_LENGTH = 32;
    private static final int MIN_MINIBLOCK_LENGTH = 8;

    private long[] valueBuffer = new long[MAX_BLOCK_LENGTH + 1];
    private int position = 0;
    private long[] deltas = new long[MAX_BLOCK_LENGTH];
    private long minDelta = 0;
    private int [] referenceFrame = new int[MAX_BLOCK_LENGTH];
    private int numEncodedValues = 0;

    private MemoryOutputStream bestEncoding = new MemoryOutputStream(128);
    private MemoryOutputStream currentEncoding = new MemoryOutputStream(128);

    @Override
    public void encode(Object o) {
      numValues += 1;
      if (position == MAX_BLOCK_LENGTH + 1) {
        flushBlock();
      }
      valueBuffer[position] = (int)o;
      position += 1;
    }

    private MemoryOutputStream getBestMiniblockEncodingForBlock() {
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
          MemoryOutputStream tmp = bestEncoding;
          bestEncoding = currentEncoding;
          currentEncoding = tmp;
          minLength = currentLength;
        }
        miniblockLength <<= 1;
      }

      return bestEncoding;
    }

    private void flushBlockWithNumMiniBlocks(MemoryOutputStream memoryOutputStream, int miniblockLength,
                                             int blockLength) {
      int numMiniblocks = blockLength / miniblockLength;
      int startValue = (int)valueBuffer[0];
      int[] miniblockBitWidths = new int[numMiniblocks];
      for (int j=0; j<numMiniblocks; ++j) {
        miniblockBitWidths[j] = getMiniBlockBitWidth(j * miniblockLength, miniblockLength);
      }
      Bytes.writeUInt(memoryOutputStream, blockLength);
      Bytes.writeUInt(memoryOutputStream, numMiniblocks);
      Bytes.writeUInt(memoryOutputStream, position);
      Bytes.writeSInt(memoryOutputStream, startValue);
      if (numMiniblocks > 0){
        Bytes.writeSLong(memoryOutputStream, minDelta);
        for (int j=0; j<numMiniblocks; ++j) {
          memoryOutputStream.write(miniblockBitWidths[j]);
        }
        for (int j=0; j<numMiniblocks; ++j) {
          int numRemainingValues = position - 1 - j * miniblockLength;
          int length = numRemainingValues < miniblockLength ? numRemainingValues : miniblockLength;
          Bytes.writePackedInts32(memoryOutputStream, referenceFrame, miniblockBitWidths[j],
                                  j * miniblockLength, length);
        }
      }
    }

    private int getMiniBlockBitWidth(int miniBlockStartPosition, int miniBlockLength) {
      int maxBitWidth = 0;
      for (int j=miniBlockStartPosition; j<miniBlockStartPosition+miniBlockLength; ++j) {
        int bitWidth = Bytes.getBitWidth(referenceFrame[j]);
        if (bitWidth > maxBitWidth) {
          maxBitWidth = bitWidth;
        }
      }
      return maxBitWidth;
    }

    private void computeDeltas(int blockLength) {
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

    private static int getBlockLength(int numValuesInBlock) {
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
        MemoryOutputStream fullBlockEncoding = getBestMiniblockEncodingForBlock();
        fullBlockEncoding.writeTo(mos);
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

  public final static IDecoderFactory decoderFactory = new IDecoderFactory() {
      @Override
      public IDecoder create(ByteBuffer bb) {
        return new Decoder(bb);
      }
    };

}
