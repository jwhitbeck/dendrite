package dendrite.java;

public class Int32PackedDeltaDecoder implements Int32Decoder {

  private final ByteArrayReader byte_array_reader;
  private int[] miniblock_buffer = new int[128];
  private int miniblock_position = 0;
  private int miniblock_size = 0;
  private int current_miniblock_index = 0;
  private int num_miniblocks = 0;
  private int[] miniblock_bit_widths = new int[32];
  private int remaining_values_in_block = 0;
  private int block_size = 0;
  private long block_min_delta = 0;
  private long block_current_value = 0;

  public Int32PackedDeltaDecoder(final ByteArrayReader baw) {
    byte_array_reader = baw;
  }

  @Override
  public int decode() {
    if (remaining_values_in_block > 0) {
      if (miniblock_position == -1) { // read from first value
        miniblock_position = 0;
      } else if (current_miniblock_index == -1) { // no miniblock loaded
        initNextMiniBlock();
        setCurrentValueFromMiniBlockBuffer();
      } else if (miniblock_position < miniblock_size) { // reading from buffered miniblock
        setCurrentValueFromMiniBlockBuffer();
      } else { // finished reading current mini block
        initNextMiniBlock();
        setCurrentValueFromMiniBlockBuffer();
      }
      remaining_values_in_block -= 1;
      return (int)block_current_value;
    } else { // no more values in block, load next block
      initNextBlock();
      return decode();
    }
  }

  private void setCurrentValueFromMiniBlockBuffer() {
    long next_relative_delta = miniblock_buffer[miniblock_position] & 0xffffffffL;
    long delta = next_relative_delta + block_min_delta;
    block_current_value += delta;
    miniblock_position += 1;
  }

  private void initNextMiniBlock() {
    current_miniblock_index += 1;
    int width = miniblock_bit_widths[current_miniblock_index];
    int length = remaining_values_in_block < miniblock_size? remaining_values_in_block : miniblock_size;
    byte_array_reader.readPackedInts32(miniblock_buffer, width, length);
    miniblock_position = 0;
  }

  private void ensureMiniBlocksSizeInSufficient(final int miniblock_size) {
    if (miniblock_buffer.length < miniblock_size) {
      miniblock_buffer = new int[miniblock_size];
    }
  }

  private void ensureMiniBlocksBitWidthsSizeIsSufficient(final int num_miniblocks) {
    if (miniblock_bit_widths.length < num_miniblocks) {
      miniblock_bit_widths = new int[num_miniblocks];
    }
  }

  private void initNextBlock() {
    block_size = byte_array_reader.readUInt32();
    num_miniblocks = byte_array_reader.readUInt32();
    miniblock_size = num_miniblocks > 0? block_size / num_miniblocks : 0;
    ensureMiniBlocksSizeInSufficient(miniblock_size);
    ensureMiniBlocksBitWidthsSizeIsSufficient(num_miniblocks);
    remaining_values_in_block = byte_array_reader.readUInt32();
    miniblock_position = -1;
    current_miniblock_index = -1;
    block_current_value = byte_array_reader.readSInt32();
    if (num_miniblocks > 0) {
      block_min_delta = byte_array_reader.readSInt64();
      for (int i=0; i<num_miniblocks; ++i) {
        miniblock_bit_widths[i] = (int)byte_array_reader.readByte() & 0xff;
      }
    }
  }

}
