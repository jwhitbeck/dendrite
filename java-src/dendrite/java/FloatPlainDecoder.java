package dendrite.java;

public class FloatPlainDecoder implements FloatDecoder {

  private final ByteArrayReader byte_array_reader;

  public FloatPlainDecoder(final ByteArrayReader baw) {
    byte_array_reader = baw;
  }

  @Override
  public float decode() {
    return byte_array_reader.readFloat();
  }

}
