package dendrite.java;

public class LongPlainEncoder extends AbstractEncoder {

  @Override
  public void encode(final Object o) {
    num_values += 1;
    byte_array_writer.writeFixedLong((long)o);
  }

}
