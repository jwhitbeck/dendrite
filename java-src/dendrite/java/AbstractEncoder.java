package dendrite.java;

public abstract class AbstractEncoder implements BufferedByteArrayWriter {

  protected final ByteArrayWriter byte_array_writer;

  public AbstractEncoder() {
    byte_array_writer = new ByteArrayWriter();
  }

  @Override
  public void reset() {
    byte_array_writer.reset();
  }

  @Override
  public void finish() {}

  @Override
  public int size() {
    return byte_array_writer.size();
  }

  @Override
  public int estimatedSize() {
    return size();
  }

  @Override
  public void writeTo(final ByteArrayWriter baw) {
    finish();
    byte_array_writer.writeTo(baw);
  }

}
