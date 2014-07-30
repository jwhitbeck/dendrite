package dendrite.java;

public class ByteArrayDeltaLengthEncoder implements Encoder {

  private final IntPackedDeltaEncoder lengths_encoder;
  private final ByteArrayWriter byte_array_writer;
  private int num_values = 0;

  public ByteArrayDeltaLengthEncoder() {
    lengths_encoder = new IntPackedDeltaEncoder();
    byte_array_writer = new ByteArrayWriter();
  }

  @Override
  public void encode(final Object o) {
    final byte[] bs = (byte[]) o;
    num_values += 1;
    lengths_encoder.encode(bs.length);
    byte_array_writer.writeByteArray(bs, 0, bs.length);
  }

  public void encode(final byte[] bs, final int offset, final int length) {
    num_values += 1;
    lengths_encoder.encode(length);
    byte_array_writer.writeByteArray(bs, offset, length);
  }

  @Override
  public void reset() {
    num_values = 0;
    byte_array_writer.reset();
    lengths_encoder.reset();
  }

  @Override
  public void finish() {
    lengths_encoder.finish();
  }

  @Override
  public int length() {
    return ByteArrayWriter.getNumUIntBytes(num_values)
      + ByteArrayWriter.getNumUIntBytes(lengths_encoder.length()) + lengths_encoder.length()
      + byte_array_writer.length();
  }

  public int estimatedLength() {
    int estimated_lengths_encoder_length = lengths_encoder.estimatedLength();
    return ByteArrayWriter.getNumUIntBytes(num_values) + byte_array_writer.length()
      + ByteArrayWriter.getNumUIntBytes(estimated_lengths_encoder_length) + estimated_lengths_encoder_length;
  }

  @Override
  public void flush(final ByteArrayWriter baw) {
    finish();
    baw.writeUInt(num_values);
    baw.writeUInt(lengths_encoder.length());
    lengths_encoder.flush(baw);
    byte_array_writer.flush(baw);
  }

  @Override
  public int numEncodedValues() {
    return num_values;
  }

}
