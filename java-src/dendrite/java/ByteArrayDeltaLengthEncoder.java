package dendrite.java;

public class ByteArrayDeltaLengthEncoder implements ByteArrayEncoder {

  private final Int32PackedDeltaEncoder lengths_encoder;
  private final ByteArrayWriter byte_array_writer;

  public ByteArrayDeltaLengthEncoder() {
    lengths_encoder = new Int32PackedDeltaEncoder();
    byte_array_writer = new ByteArrayWriter();
  }

  @Override
  public void append(final byte[] bs) {
    lengths_encoder.append(bs.length);
    byte_array_writer.writeByteArray(bs, 0, bs.length);
  }

  public void append(final byte[] bs, final int offset, final int length) {
    lengths_encoder.append(length);
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
    return ByteArrayWriter.getNumUInt32Bytes(lengths_encoder.size())
      + lengths_encoder.size() + byte_array_writer.size();
  }

  @Override
  public void writeTo(final ByteArrayWriter baw) {
    finish();
    baw.writeUInt32(lengths_encoder.size());
    lengths_encoder.writeTo(baw);
    byte_array_writer.writeTo(baw);
  }

}
