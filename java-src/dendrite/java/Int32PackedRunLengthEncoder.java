package dendrite.java;

public class Int32PackedRunLengthEncoder implements Int32Encoder {

  private final ByteArrayWriter int_buffer;
  private final Int32FixedBitWidthPackedRunLengthEncoder rle_encoder;
  private int num_buffered_values;
  private int max_width;
  private boolean is_finished;

  public Int32PackedRunLengthEncoder() {
    rle_encoder = new Int32FixedBitWidthPackedRunLengthEncoder(0);
    int_buffer = new ByteArrayWriter();
    max_width = 0;
    num_buffered_values = 0;
    is_finished = false;
  }

  @Override
  public void encode(final int i) {
    int width = ByteArrayWriter.getBitWidth(i);
    if (width > max_width) {
      max_width = width;
    }
    int_buffer.writeUInt32(i);
    num_buffered_values += 1;
  }

  @Override
  public void reset() {
    rle_encoder.reset();
    int_buffer.reset();
    max_width = 0;
    num_buffered_values = 0;
    is_finished = false;
  }

  @Override
  public void finish() {
    if (!is_finished) {
      rle_encoder.setWidth(max_width);
      ByteArrayReader int_buffer_reader = new ByteArrayReader(int_buffer.buffer);
      for (int j=0; j<num_buffered_values; ++j) {
        rle_encoder.encode(int_buffer_reader.readUInt32());
      }
      rle_encoder.finish();
      is_finished = true;
    }
  }

  @Override
  public int size() {
    return 1 + rle_encoder.size();
  }

  @Override
  public int estimatedSize() {
    int estimated_num_octoplets = (num_buffered_values / 8) + 1;
    return 1 + ByteArrayWriter.getNumUInt32Bytes(estimated_num_octoplets << 1)
      + (8 * estimated_num_octoplets * max_width);
  }

  @Override
  public void writeTo(final ByteArrayWriter baw) {
    finish();
    baw.writeByte((byte)max_width);
    rle_encoder.writeTo(baw);
  }

}
