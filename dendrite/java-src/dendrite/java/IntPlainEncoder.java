package dendrite.java;

public class IntPlainEncoder extends AbstractEncoder implements IntEncoder {

  @Override
  public void encode(int i) {
    byte_array_writer.writeFixedInt(i);
  }

}
