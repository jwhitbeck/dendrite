package dendrite.java;

public class Int64PlainDecoder implements Int64Decoder {

  private final ByteArrayReader byte_array_reader;

  public Int64PlainDecoder(final ByteArrayReader baw) {
    byte_array_reader = baw;
  }

  @Override
  public long decode() {
    return byte_array_reader.readFixedInt64();
  }

}
