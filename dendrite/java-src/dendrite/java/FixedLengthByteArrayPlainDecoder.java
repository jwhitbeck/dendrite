package dendrite.java;

public class FixedLengthByteArrayPlainDecoder extends AbstractDecoder {

  private final int length;

  public FixedLengthByteArrayPlainDecoder(final ByteArrayReader baw) {
    super(baw);
    this.length = byte_array_reader.readUInt();
  }

  @Override
  public Object decode() {
    byte[] fixed_length_byte_array = new byte[length];
    byte_array_reader.readByteArray(fixed_length_byte_array, 0, length);
    return fixed_length_byte_array;
  }

}
