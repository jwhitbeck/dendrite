package dendrite.java;

public class ByteArrayDeltaLengthDecoder implements ByteArrayDecoder {

  private final ByteArrayReader byte_array_reader;
  private final IntPackedDeltaDecoder lengths_decoder;

  public ByteArrayDeltaLengthDecoder(final ByteArrayReader baw) {
    int lengths_num_bytes = baw.readUInt();
    lengths_decoder = new IntPackedDeltaDecoder(baw);
    byte_array_reader = baw.sliceAhead(lengths_num_bytes);
  }

  @Override
  public byte[] decode() {
    int length = lengths_decoder.decode();
    byte[] byte_array = new byte[length];
    byte_array_reader.readByteArray(byte_array, 0, length);
    return byte_array;
  }

  public void decodeInto(ByteArrayWriter baw) {
    int length = lengths_decoder.decode();
    byte_array_reader.readBytes(baw, length);
  }

}
