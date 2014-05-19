package dendrite.java;

public class IntPlainDecoder implements IntDecoder {

  private final ByteArrayReader byte_array_reader;

  public IntPlainDecoder(final ByteArrayReader baw) {
    byte_array_reader = baw;
  }

  @Override
  public int decode() {
    return byte_array_reader.readFixedInt();
  }

}
