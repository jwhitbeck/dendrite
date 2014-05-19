package dendrite.java;

public class ByteArrayDeltaLengthEncoder implements ByteArrayEncoder {

  private final IntPackedDeltaEncoder lengths_encoder;
  private final ByteArrayWriter byte_array_writer;

  public ByteArrayDeltaLengthEncoder() {
    lengths_encoder = new IntPackedDeltaEncoder();
    byte_array_writer = new ByteArrayWriter();
  }

  @Override
  public void encode(final byte[] bs) {
    lengths_encoder.encode(bs.length);
    byte_array_writer.writeByteArray(bs, 0, bs.length);
  }

  public void encode(final byte[] bs, final int offset, final int length) {
    lengths_encoder.encode(length);
    byte_array_writer.writeByteArray(bs, offset, length);
  }

  @Override
  public void reset() {
    byte_array_writer.reset();
    lengths_encoder.reset();
  }

  @Override
  public void finish() {
    lengths_encoder.finish();
  }

  @Override
  public int size() {
    return ByteArrayWriter.getNumUIntBytes(lengths_encoder.size())
      + lengths_encoder.size() + byte_array_writer.size();
  }

  public int estimatedSize() {
    int estimated_lengths_encoder_size = lengths_encoder.estimatedSize();
    return byte_array_writer.size() + ByteArrayWriter.getNumUIntBytes(estimated_lengths_encoder_size)
      + estimated_lengths_encoder_size;
  }

  @Override
  public void writeTo(final ByteArrayWriter baw) {
    finish();
    baw.writeUInt(lengths_encoder.size());
    lengths_encoder.writeTo(baw);
    byte_array_writer.writeTo(baw);
  }

}
