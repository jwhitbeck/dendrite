package dendrite.java;

public class Int64PlainEncoder extends AbstractEncoder implements Int64Encoder {

  @Override
  public void encode(final long l) {
    byte_array_writer.writeFixedInt64(l);
  }

}
