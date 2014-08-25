package dendrite.java;

public class IntPackedRunLengthEncoder extends AbstractEncoder {

  private final ByteArrayWriter intBuffer;
  private final IntFixedBitWidthPackedRunLengthEncoder rleEncoder;
  private int numBufferedValues;
  private int maxWidth;
  private boolean isFinished;

  public IntPackedRunLengthEncoder() {
    rleEncoder = new IntFixedBitWidthPackedRunLengthEncoder(0);
    intBuffer = new ByteArrayWriter();
    maxWidth = 0;
    numBufferedValues = 0;
    isFinished = false;
  }

  @Override
  public void encode(final Object o) {
    final int i = (int) o;
    final int width = ByteArrayWriter.getBitWidth(i);
    if (width > maxWidth) {
      maxWidth = width;
    }
    intBuffer.writeUInt(i);
    numBufferedValues += 1;
  }

  @Override
  public void reset() {
    rleEncoder.reset();
    intBuffer.reset();
    maxWidth = 0;
    numBufferedValues = 0;
    isFinished = false;
  }

  @Override
  public void finish() {
    if (!isFinished) {
      rleEncoder.setWidth(maxWidth);
      ByteArrayReader intBufferReader = new ByteArrayReader(intBuffer.buffer);
      for (int j=0; j<numBufferedValues; ++j) {
        rleEncoder.encode(intBufferReader.readUInt());
      }
      rleEncoder.finish();
      isFinished = true;
    }
  }

  @Override
  public int length() {
    return 1 + rleEncoder.length();
  }

  @Override
  public int estimatedLength() {
    int estimatedNumOctoplets = (numBufferedValues / 8) + 1;
    return 1 + ByteArrayWriter.getNumUIntBytes(estimatedNumOctoplets << 1)
      + (8 * estimatedNumOctoplets * maxWidth) + ByteArrayWriter.getNumUIntBytes(numBufferedValues);
  }

  @Override
  public void flush(final ByteArrayWriter baw) {
    finish();
    baw.writeByte((byte)maxWidth);
    rleEncoder.flush(baw);
  }

  @Override
  public int numEncodedValues() {
    return numBufferedValues;
  }

}
