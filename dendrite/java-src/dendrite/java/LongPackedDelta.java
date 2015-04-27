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

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;

public final class LongPackedDelta {

  public final static class Decoder extends ADecoder {

    private long[] miniblockBuffer = new long[128];
    private int miniblockPosition = 0;
    private int miniblockLength = 0;
    private int currentMiniblockIndex = 0;
    private int numMiniblocks = 0;
    private int[] miniblockBitWidths = new int[16];
    private int remainingValuesInBlock = 0;
    private int blockLength = 0;
    private BigInteger blockMinDelta;
    private BigInteger blockCurrentValue;

    public Decoder(ByteBuffer byteBuffer) {
      super(byteBuffer);
    }

    @Override
    public Object decode() {
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
        return blockCurrentValue.longValue();
      } else { // no more values in block, load next block
        initNextBlock();
        return decode();
      }
    }

    private static BigInteger valueOfUnsignedLong(long l) {
      if (l >= 0) {
        return BigInteger.valueOf(l);
      } else {
        return BigInteger.valueOf(l & 0x7fffffffffffffffL).or(BigInteger.ZERO.flipBit(63));
      }
    }

    private void setCurrentValueFromMiniBlockBuffer() {
      BigInteger nextRelativeDelta = valueOfUnsignedLong(miniblockBuffer[miniblockPosition]);
      BigInteger delta = nextRelativeDelta.add(blockMinDelta);
      blockCurrentValue = blockCurrentValue.add(delta);
      miniblockPosition += 1;
    }

    private void initNextMiniBlock() {
      currentMiniblockIndex += 1;
      int width = miniblockBitWidths[currentMiniblockIndex];
      int length = remainingValuesInBlock < miniblockLength? remainingValuesInBlock : miniblockLength;
      Bytes.readPackedInts64(bb, miniblockBuffer, width, length);
      miniblockPosition = 0;
    }

    private void initNextBlock() {
      blockLength = Bytes.readUInt(bb);
      numMiniblocks = Bytes.readUInt(bb);
      miniblockLength = numMiniblocks > 0? blockLength / numMiniblocks : 0;
      remainingValuesInBlock = Bytes.readUInt(bb);
      miniblockPosition = -1;
      currentMiniblockIndex = -1;
      blockCurrentValue = BigInteger.valueOf(Bytes.readSLong(bb));
      if (numMiniblocks > 0) {
        blockMinDelta = Bytes.readSIntVLQ(bb);
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

    private BigInteger[] valueBuffer = new BigInteger[MAX_BLOCK_LENGTH + 1];
    private int position = 0;
    private BigInteger[] deltas = new BigInteger[MAX_BLOCK_LENGTH];
    private BigInteger minDelta;
    private long [] referenceFrame = new long[MAX_BLOCK_LENGTH];
    private int numEncodedValues = 0;

    private MemoryOutputStream bestEncoding = new MemoryOutputStream(128);
    private MemoryOutputStream currentEncoding = new MemoryOutputStream(128);

    @Override
    public void encode(Object o) {
      numValues += 1;
      if (position == MAX_BLOCK_LENGTH + 1) {
        flushBlock();
      }
      valueBuffer[position] = BigInteger.valueOf((long)o);
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

    private void flushBlockWithNumMiniBlocks(MemoryOutputStream memoryOutputStream, int miniBlockLength,
                                             int blockLength) {
      int numMiniblocks = blockLength / miniBlockLength;
      long startValue = valueBuffer[0].longValue();
      int[] miniblockBitWidths = new int[numMiniblocks];
      for (int j=0; j<numMiniblocks; ++j) {
        miniblockBitWidths[j] = getMiniBlockBitWidth(j * miniBlockLength, miniBlockLength);
      }
      Bytes.writeUInt(memoryOutputStream, blockLength);
      Bytes.writeUInt(memoryOutputStream, numMiniblocks);
      Bytes.writeUInt(memoryOutputStream, position);
      Bytes.writeSLong(memoryOutputStream, startValue);
      if (numMiniblocks > 0){
        Bytes.writeSIntVLQ(memoryOutputStream, minDelta);
        for (int j=0; j<numMiniblocks; ++j) {
          memoryOutputStream.write(miniblockBitWidths[j]);
        }
        for (int j=0; j<numMiniblocks; ++j) {
          int numRemainingValues = position - 1 - j * miniBlockLength;
          int length = numRemainingValues < miniBlockLength ? numRemainingValues : miniBlockLength;
          Bytes.writePackedInts64(memoryOutputStream, referenceFrame, miniblockBitWidths[j],
                                  j * miniBlockLength, length);
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

    private static final BigInteger INFINITY = BigInteger.valueOf(Long.MAX_VALUE).shiftLeft(2);

    private void computeDeltas(int blockLength) {
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
