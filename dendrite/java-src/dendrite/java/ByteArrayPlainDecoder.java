package dendrite.java;

public class ByteArrayPlainDecoder extends AbstractDecoder {

  public ByteArrayPlainDecoder(final ByteArrayReader baw) {
    super(baw);
  }

  @Override
  public Object decode() {
    int length = byte_array_reader.readFixedInt();
    byte[] byte_array = new byte[length];
    byte_array_reader.readByteArray(byte_array, 0, length);
    return byte_array;
  }

}
