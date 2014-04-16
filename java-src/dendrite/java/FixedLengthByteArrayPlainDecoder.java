package dendrite.java;

public class FixedLengthByteArrayPlainDecoder implements FixedLengthByteArrayDecoder {

  private final ByteArrayReader byte_array_reader;
  private final int length;

  public FixedLengthByteArrayPlainDecoder(final ByteArrayReader baw) {
    byte_array_reader = baw;
    this.length = baw.readUInt32();
  }

  @Override
  public byte[] decode() {
    byte[] fixed_length_byte_array = new byte[length];
    byte_array_reader.readByteArray(fixed_length_byte_array, 0, length);
    return fixed_length_byte_array;
  }

}
