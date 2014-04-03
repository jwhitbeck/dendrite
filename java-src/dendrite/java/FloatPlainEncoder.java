package dendrite.java;

public class FloatPlainEncoder extends AbstractEncoder implements FloatEncoder {

  @Override
  public void append(final float f) {
    byte_array_writer.writeFloat(f);
  }

}
