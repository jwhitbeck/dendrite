package dendrite.java;

public class FloatPlainDecoder extends AbstractDecoder {

  public FloatPlainDecoder(final ByteArrayReader baw) {
    super(baw);
  }

  @Override
  public Object decode() {
    return byte_array_reader.readFloat();
  }

}
