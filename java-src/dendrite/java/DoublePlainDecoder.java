package dendrite.java;

public class DoublePlainDecoder implements DoubleDecoder {

  private final ByteArrayReader byte_array_reader;

  public DoublePlainDecoder(final ByteArrayReader baw) {
    byte_array_reader = baw;
  }

  @Override
  public double decode() {
    return byte_array_reader.readDouble();
  }

}
