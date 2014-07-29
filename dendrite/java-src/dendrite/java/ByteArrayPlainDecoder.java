package dendrite.java;

public class ByteArrayPlainDecoder implements ByteArrayDecoder {

  private final ByteArrayReader byte_array_reader;

  public ByteArrayPlainDecoder(final ByteArrayReader baw) {
    byte_array_reader = baw;
  }

  @Override
  public byte[] decode() {
    int length = byte_array_reader.readFixedInt();
    byte[] byte_array = new byte[length];
    byte_array_reader.readByteArray(byte_array, 0, length);
    return byte_array;
  }

}
