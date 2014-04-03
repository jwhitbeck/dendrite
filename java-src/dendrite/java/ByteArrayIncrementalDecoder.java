package dendrite.java;

public class ByteArrayIncrementalDecoder implements ByteArrayDecoder {

  private final ByteArrayDeltaLengthDecoder byte_array_decoder;
  private final Int32PackedDeltaDecoder prefix_lengths_decoder;
  private final ByteArrayWriter buffer;

  public ByteArrayIncrementalDecoder(final ByteArrayReader baw) {
    int prefix_lengths_num_bytes = baw.readUInt32();
    prefix_lengths_decoder = new Int32PackedDeltaDecoder(baw);
    byte_array_decoder = new ByteArrayDeltaLengthDecoder(baw.sliceAhead(prefix_lengths_num_bytes));
    buffer = new ByteArrayWriter(128);
  }

  @Override
  public byte[] next() {
    int prefix_length = prefix_lengths_decoder.next();
    buffer.resetTo(prefix_length);
    byte_array_decoder.readNextInto(buffer);
    byte[] byte_array = new byte[buffer.size()];
    System.arraycopy(buffer.buffer, 0, byte_array, 0, buffer.size());
    return byte_array;
  }

}
