package dendrite.java;

public class ByteArrayIncrementalEncoder implements ByteArrayEncoder {

  private byte[] previous_byte_array = null;
  private final ByteArrayDeltaLengthEncoder byte_array_encoder;
  private final IntPackedDeltaEncoder prefix_lengths_encoder;

  public ByteArrayIncrementalEncoder() {
    prefix_lengths_encoder = new IntPackedDeltaEncoder();
    byte_array_encoder = new ByteArrayDeltaLengthEncoder();
  }

  @Override
  public void encode(final byte[] bs) {
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
    prefix_lengths_encoder.encode(first_different_byte_idx);
    byte_array_encoder.encode(bs, first_different_byte_idx, bs.length-first_different_byte_idx);
    previous_byte_array = bs;
  }

  @Override
  public void reset() {
    prefix_lengths_encoder.reset();
    byte_array_encoder.reset();
    previous_byte_array = null;
  }

  @Override
  public void finish() {
    prefix_lengths_encoder.finish();
    byte_array_encoder.finish();
  }

  @Override
  public int length() {
    return ByteArrayWriter.getNumUIntBytes(prefix_lengths_encoder.length())
      + prefix_lengths_encoder.length() + byte_array_encoder.length();
  }

  @Override
  public int estimatedLength() {
    int estimated_prefix_lengths_encoder_length = prefix_lengths_encoder.estimatedLength();
    return ByteArrayWriter.getNumUIntBytes(estimated_prefix_lengths_encoder_length)
      + estimated_prefix_lengths_encoder_length + byte_array_encoder.length();
  }

  @Override
  public void writeTo(final ByteArrayWriter baw) {
    finish();
    baw.writeUInt(prefix_lengths_encoder.length());
    prefix_lengths_encoder.writeTo(baw);
    byte_array_encoder.writeTo(baw);
  }

}
