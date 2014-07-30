package dendrite.java;

public class ByteArrayDeltaLengthDecoder extends AbstractDecoder {

  private final IntPackedDeltaDecoder lengths_decoder;

  public ByteArrayDeltaLengthDecoder(final ByteArrayReader baw) {
    super(baw);
    int lengths_num_bytes = byte_array_reader.readUInt();
    lengths_decoder = new IntPackedDeltaDecoder(byte_array_reader);
    byte_array_reader.position += lengths_num_bytes;
  }

  @Override
  public Object decode() {
    int length = (int)lengths_decoder.decode();
    byte[] byte_array = new byte[length];
    byte_array_reader.readByteArray(byte_array, 0, length);
    return byte_array;
  }

  public void decodeInto(ByteArrayWriter baw) {
    int length = (int)lengths_decoder.decode();
    byte_array_reader.readBytes(baw, length);
  }

}
