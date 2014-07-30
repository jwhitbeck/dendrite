package dendrite.java;

public class IntPlainEncoder extends AbstractEncoder {

  @Override
  public void encode(final Object o) {
    num_values += 1;
    byte_array_writer.writeFixedInt((int) o);
  }

}
