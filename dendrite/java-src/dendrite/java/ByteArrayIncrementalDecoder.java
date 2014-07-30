package dendrite.java;

public class ByteArrayIncrementalDecoder extends AbstractDecoder {

  private final ByteArrayDeltaLengthDecoder byte_array_decoder;
  private final IntPackedDeltaDecoder prefix_lengths_decoder;
  private final ByteArrayWriter buffer;

  public ByteArrayIncrementalDecoder(final ByteArrayReader baw) {
    super(baw);
    int prefix_lengths_num_bytes = byte_array_reader.readUInt();
    prefix_lengths_decoder = new IntPackedDeltaDecoder(byte_array_reader);
    byte_array_decoder =
      new ByteArrayDeltaLengthDecoder(byte_array_reader.sliceAhead(prefix_lengths_num_bytes));
    buffer = new ByteArrayWriter(128);
  }

  @Override
  public Object decode() {
    int prefix_length = (int)prefix_lengths_decoder.decode();
    buffer.position = prefix_length;
    byte_array_decoder.decodeInto(buffer);
    byte[] byte_array = new byte[buffer.length()];
    System.arraycopy(buffer.buffer, 0, byte_array, 0, buffer.length());
    return byte_array;
  }

}
