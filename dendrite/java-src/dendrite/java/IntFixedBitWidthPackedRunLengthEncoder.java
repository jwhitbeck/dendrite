package dendrite.java;

public class IntFixedBitWidthPackedRunLengthEncoder extends AbstractEncoder {

  private int rle_value = 0;
  private int num_occurences_rle_value = 0;
  private int[] current_octuplet = new int[8];
  private int current_octuplet_position = 0;
  private ByteArrayWriter octuplet_buffer = new ByteArrayWriter();
  private int num_buffered_octuplets;
  private int rle_threshold;
  private int width;

  public IntFixedBitWidthPackedRunLengthEncoder(final int width) {
    setWidth(width);
  }

  protected void setWidth(final int width) {
    this.width = width;
    rle_threshold = computeRLEThreshold(width);
  }

  private static int computeRLEThreshold(final int width) {
    int rle_run_num_bytes = 2 + (width / 8);
    double num_packed_values_per_byte = (double)8 / (double)width;
    return (int)(rle_run_num_bytes * num_packed_values_per_byte) + 1;
  }

  private void encodeInt(final int i) {
    if (current_octuplet_position == 0) {
      if (num_occurences_rle_value == 0) {
        startRLERun(i);
      } else if (rle_value == i) {
        num_occurences_rle_value += 1;
      } else if (num_occurences_rle_value >= rle_threshold) {
        flushRLE();
        encodeInt(i);
      } else {
        packRLERun();
        encodeInt(i);
      }
    } else {
      bufferPackedInt(i);
    }
  }

  @Override
  public void encode(final Object o) {
    num_values += 1;
    encodeInt((int)o);
  }

  @Override
  public void reset() {
    super.reset();
    rle_value = 0;
    octuplet_buffer.reset();
    current_octuplet_position = 0;
    num_occurences_rle_value = 0;
    num_buffered_octuplets = 0;
  }

  @Override
  public void finish() {
    if (current_octuplet_position > 0) {
      for (int j=current_octuplet_position; j<8; j++) {
        current_octuplet[j] = 0; // pad with zeros
      }
      current_octuplet_position = 0;
      octuplet_buffer.writePackedInts32(current_octuplet, width, 8);
      num_buffered_octuplets += 1;
      flushBitPacked();
    } else if (num_occurences_rle_value > 0) {
      flushRLE();
    } else if (num_buffered_octuplets > 0) {
      flushBitPacked();
    }
  }

  @Override
  public int estimatedLength() {
    if (num_occurences_rle_value > 0) {
      return super.estimatedLength() + ByteArrayWriter.getNumUIntBytes(rle_value)
        + ByteArrayWriter.getBitWidth(num_occurences_rle_value << 1);
    } else {
      return super.estimatedLength() + ByteArrayWriter.getNumUIntBytes(num_buffered_octuplets << 1)
        + (8 * width * num_buffered_octuplets) + current_octuplet_position > 0? (8 * width) : 0;
    }
  }

  private void packRLERun() {
    for (int j=0; j<num_occurences_rle_value; ++j) {
      bufferPackedInt(rle_value);
    }
    num_occurences_rle_value = 0;
  }

  private void startRLERun(final int i) {
    num_occurences_rle_value = 1;
    rle_value = i;
  }

  private void bufferPackedInt(final int i) {
    current_octuplet[current_octuplet_position] = i;
    current_octuplet_position += 1;
    if (current_octuplet_position == 8) {
      octuplet_buffer.writePackedInts32(current_octuplet, width, 8);
      current_octuplet_position = 0;
      num_buffered_octuplets += 1;
    }
  }

  private void flushBitPacked() {
    writeBitPackedHeader();
    flushBitPackedBuffer();
    num_buffered_octuplets = 0;
  }

  private void writeBitPackedHeader() {
    byte_array_writer.writeUInt(num_buffered_octuplets << 1 | 1);
  }

  private void flushBitPackedBuffer() {
    octuplet_buffer.flush(byte_array_writer);
    octuplet_buffer.reset();
  }

  private void flushRLE() {
    if (num_buffered_octuplets > 0){
      flushBitPacked();
    }
    writeRLEHeader();
    writeRLEValue();
    num_occurences_rle_value = 0;
  }

  private void writeRLEHeader() {
    byte_array_writer.writeUInt(num_occurences_rle_value << 1);
  }

  private void writeRLEValue () {
    byte_array_writer.writePackedInt(rle_value, width);
  }

}
