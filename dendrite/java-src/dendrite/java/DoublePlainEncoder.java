package dendrite.java;

public class DoublePlainEncoder extends AbstractEncoder {

  @Override
  public void encode(final Object o) {
    num_values += 1;
    byte_array_writer.writeDouble((double)o);
  }

}
