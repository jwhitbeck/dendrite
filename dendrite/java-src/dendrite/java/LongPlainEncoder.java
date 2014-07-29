package dendrite.java;

public class LongPlainEncoder extends AbstractEncoder implements LongEncoder {

  @Override
  public void encode(final long l) {
    byte_array_writer.writeFixedLong(l);
  }

}
