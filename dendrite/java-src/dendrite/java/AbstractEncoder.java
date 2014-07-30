package dendrite.java;

public abstract class AbstractEncoder implements Encoder {

  protected final ByteArrayWriter byte_array_writer;
  protected int num_values = 0;

  public AbstractEncoder() {
    byte_array_writer = new ByteArrayWriter();
  }

  @Override
  public void reset() {
    num_values = 0;
    byte_array_writer.reset();
  }

  @Override
  public void finish() {}

  @Override
  public int length() {
    return ByteArrayWriter.getNumUIntBytes(num_values) +  byte_array_writer.length();
  }

  @Override
  public int estimatedLength() {
    return length();
  }

  @Override
  public void flush(final ByteArrayWriter baw) {
    finish();
    baw.writeUInt(num_values);
    byte_array_writer.flush(baw);
  }

}
