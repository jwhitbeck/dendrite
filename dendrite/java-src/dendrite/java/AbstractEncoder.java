package dendrite.java;

public abstract class AbstractEncoder implements Encoder {

  protected final ByteArrayWriter byteArrayWriter;
  protected int numValues = 0;

  public AbstractEncoder() {
    this.byteArrayWriter = new ByteArrayWriter();
  }

  @Override
  public void reset() {
    numValues = 0;
    byteArrayWriter.reset();
  }

  @Override
  public void finish() {}

  @Override
  public int length() {
    return ByteArrayWriter.getNumUIntBytes(numValues) +  byteArrayWriter.length();
  }

  @Override
  public int estimatedLength() {
    return length();
  }

  @Override
  public void flush(final ByteArrayWriter baw) {
    finish();
    baw.writeUInt(numValues);
    byteArrayWriter.flush(baw);
  }

  @Override
  public int numEncodedValues() {
    return numValues;
  }

}
