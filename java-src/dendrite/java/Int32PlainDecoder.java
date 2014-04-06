package dendrite.java;

public class Int32PlainDecoder implements Int32Decoder {

  private final ByteArrayReader byte_array_reader;

  public Int32PlainDecoder(final ByteArrayReader baw) {
    byte_array_reader = baw;
  }

  @Override
  public int decode() {
    return byte_array_reader.readFixedInt32();
  }

}
