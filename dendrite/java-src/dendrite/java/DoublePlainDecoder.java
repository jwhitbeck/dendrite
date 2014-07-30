package dendrite.java;

public class DoublePlainDecoder extends AbstractDecoder {

  public DoublePlainDecoder(final ByteArrayReader baw) {
    super(baw);
  }

  @Override
  public Object decode() {
    return byte_array_reader.readDouble();
  }

}
