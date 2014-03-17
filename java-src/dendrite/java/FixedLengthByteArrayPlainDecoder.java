package dendrite.java;

public class FixedLengthByteArrayPlainDecoder implements FixedLengthByteArrayDecoder {

  private final ByteArrayReader byte_array_reader;
  private final int length;

  public FixedLengthByteArrayPlainDecoder(final ByteArrayReader baw, final int length) {
    byte_array_reader = baw;
    this.length = length;
  }

  @Override
  public byte[] next() {
    byte[] fixed_length_byte_array = new byte[length];
    byte_array_reader.readByteArray(fixed_length_byte_array, 0, length);
    return fixed_length_byte_array;
  }

}
