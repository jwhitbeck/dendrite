package dendrite.java;

public abstract class AbstractEncoder implements Encoder {

  protected final ByteArrayWriter byte_array_writer;

  public AbstractEncoder() {
    byte_array_writer = new ByteArrayWriter();
  }

  @Override
  public void reset() {
    byte_array_writer.reset();
  }

  @Override
  public void flush() {}

  @Override
  public int size() {
    return byte_array_writer.size();
  }

  @Override
  public void writeTo(final ByteArrayWriter baw) {
    flush();
    byte_array_writer.writeTo(baw);
  }

}
