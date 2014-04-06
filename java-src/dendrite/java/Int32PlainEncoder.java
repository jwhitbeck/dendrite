package dendrite.java;

public class Int32PlainEncoder extends AbstractEncoder implements Int32Encoder {

  @Override
  public void encode(int i) {
    byte_array_writer.writeFixedInt32(i);
  }

}
