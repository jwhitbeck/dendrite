package dendrite.java;

public class FloatPlainEncoder extends AbstractEncoder {

  @Override
  public void encode(final Object o) {
    num_values += 1;
    byte_array_writer.writeFloat((float)o);
  }

}
