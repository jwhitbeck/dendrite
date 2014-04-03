package dendrite.java;

public class DoublePlainEncoder extends AbstractEncoder implements DoubleEncoder {

  @Override
  public void append(final double d) {
    byte_array_writer.writeDouble(d);
  }

}
