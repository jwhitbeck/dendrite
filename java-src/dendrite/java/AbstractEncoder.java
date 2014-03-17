package dendrite.java;

public class AbstractEncoder implements Encoder {

  protected ByteArrayWriter byte_array_writer;

  public AbstractEncoder() {
    byte_array_writer = new ByteArrayWriter();
  }

  @Override
  public void reset() {
    byte_array_writer.reset();
  }

  @Override
  public void flush(final ByteArrayWriter baw) {
    byte_array_writer.copy(baw);
  }

}
