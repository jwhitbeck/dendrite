package dendrite.java;

public class Int32PackedRunLengthWithBitWidthEncoder implements Int32Encoder {

  private final ByteArrayWriter int_buffer;
  private final Int32PackedRunLengthEncoder rle_encoder;
  private int num_buffered_values;
  private int max_buffered_value;
  private int width;

  public Int32PackedRunLengthWithBitWidthEncoder() {
    rle_encoder = new Int32PackedRunLengthEncoder(0);
    int_buffer = new ByteArrayWriter();
    max_buffered_value = 0;
    num_buffered_values = 0;
  }

  @Override
  public void encode(final int i) {
    if (i > max_buffered_value) {
      max_buffered_value = i;
    }
    int_buffer.writeUInt32(i);
    num_buffered_values += 1;
  }

  @Override
  public void reset() {
    rle_encoder.reset();
    int_buffer.reset();
    max_buffered_value = 0;
    num_buffered_values = 0;
  }

  @Override
  public void finish() {
    width = ByteArrayWriter.getBitWidth(max_buffered_value);
    rle_encoder.setWidth(width);
    ByteArrayReader int_buffer_reader = new ByteArrayReader(int_buffer.buffer);
    for (int j=0; j<num_buffered_values; ++j) {
      rle_encoder.encode(int_buffer_reader.readUInt32());
    }
    rle_encoder.finish();
  }

  @Override
  public int size() {
    return ByteArrayWriter.getNumUInt32Bytes(width) + rle_encoder.size();
  }

  @Override
  public void writeTo(final ByteArrayWriter baw) {
    finish();
    baw.writeUInt32(width);
    rle_encoder.writeTo(baw);
  }

}
