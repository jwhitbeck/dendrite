package dendrite.java;

public class ByteArrayIncrementalEncoder implements ByteArrayEncoder {

  private byte[] previous_byte_array = null;
  private final ByteArrayDeltaLengthEncoder byte_array_encoder;
  private final Int32PackedDeltaEncoder prefix_lengths_encoder;

  public ByteArrayIncrementalEncoder() {
    prefix_lengths_encoder = new Int32PackedDeltaEncoder();
    byte_array_encoder = new ByteArrayDeltaLengthEncoder();
  }

  @Override
  public void append(byte[] bs) {
    int first_different_byte_idx = 0;
    if (previous_byte_array != null) {
      int i = 0;
      while (i < Math.min(bs.length, previous_byte_array.length)) {
        if (bs[i] != previous_byte_array[i]) {
          break;
        }
        i++;
      }
      first_different_byte_idx = i;
    }
    prefix_lengths_encoder.append(first_different_byte_idx);
    byte_array_encoder.append(bs, first_different_byte_idx, bs.length-first_different_byte_idx);
    previous_byte_array = bs;
  }

  @Override
  public void reset() {
    prefix_lengths_encoder.reset();
    byte_array_encoder.reset();
    previous_byte_array = null;
  }

  @Override
  public void flush() {
    prefix_lengths_encoder.flush();
    byte_array_encoder.flush();
  }

  @Override
  public int size() {
    return ByteArrayWriter.getNumUInt32Bytes(prefix_lengths_encoder.size())
      + prefix_lengths_encoder.size() + byte_array_encoder.size();
  }

  @Override
  public void writeTo(final ByteArrayWriter baw) {
    flush();
    baw.writeUInt32(prefix_lengths_encoder.size());
    prefix_lengths_encoder.writeTo(baw);
    byte_array_encoder.writeTo(baw);
  }

}
