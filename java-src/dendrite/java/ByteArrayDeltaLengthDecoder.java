package dendrite.java;

public class ByteArrayDeltaLengthDecoder implements ByteArrayDecoder {

  private final ByteArrayReader byte_array_reader;
  private final Int32PackedDeltaDecoder lengths_decoder;

  public ByteArrayDeltaLengthDecoder(final ByteArrayReader baw) {
    int lengths_num_bytes = baw.readUInt32();
    lengths_decoder = new Int32PackedDeltaDecoder(baw);
    byte_array_reader = baw.sliceAhead(lengths_num_bytes);
  }

  @Override
  public byte[] next() {
    int length = lengths_decoder.next();
    byte[] byte_array = new byte[length];
    byte_array_reader.readByteArray(byte_array, 0, length);
    return byte_array;
  }

  public void readNextInto(ByteArrayWriter baw) {
    int length = lengths_decoder.next();
    byte_array_reader.readBytes(baw, length);
  }

}
