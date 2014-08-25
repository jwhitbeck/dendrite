package dendrite.java;

import java.math.BigInteger;

public class LongPackedDeltaDecoder extends AbstractDecoder {

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

  public LongPackedDeltaDecoder(final ByteArrayReader baw) {
    super(baw);
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
    byteArrayReader.readPackedInts64(miniblockBuffer, width, length);
    miniblockPosition = 0;
  }

  private void initNextBlock() {
    blockLength = byteArrayReader.readUInt();
    numMiniblocks = byteArrayReader.readUInt();
    miniblockLength = numMiniblocks > 0? blockLength / numMiniblocks : 0;
    remainingValuesInBlock = byteArrayReader.readUInt();
    miniblockPosition = -1;
    currentMiniblockIndex = -1;
    blockCurrentValue = BigInteger.valueOf(byteArrayReader.readSLong());
    if (numMiniblocks > 0) {
      blockMinDelta = byteArrayReader.readSIntVLQ();
      for (int i=0; i<numMiniblocks; ++i) {
        miniblockBitWidths[i] = (int)byteArrayReader.readByte() & 0xff;
      }
    }
  }

}
