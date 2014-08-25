package dendrite.java;

public class IntPackedDeltaDecoder extends AbstractDecoder {

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

  public IntPackedDeltaDecoder(final ByteArrayReader baw) {
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
      return (int)blockCurrentValue;
    } else { // no more values in block, load next block
      initNextBlock();
      return decode();
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
    byteArrayReader.readPackedInts32(miniblockBuffer, width, length);
    miniblockPosition = 0;
  }

  private void initNextBlock() {
    blockLength = byteArrayReader.readUInt();
    numMiniblocks = byteArrayReader.readUInt();
    miniblockLength = numMiniblocks > 0? blockLength / numMiniblocks : 0;
    remainingValuesInBlock = byteArrayReader.readUInt();
    miniblockPosition = -1;
    currentMiniblockIndex = -1;
    blockCurrentValue = byteArrayReader.readSInt();
    if (numMiniblocks > 0) {
      blockMinDelta = byteArrayReader.readSLong();
      for (int i=0; i<numMiniblocks; ++i) {
        miniblockBitWidths[i] = (int)byteArrayReader.readByte() & 0xff;
      }
    }
  }

}
