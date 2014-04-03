package dendrite.java;

public class Int32PackedDeltaEncoder extends AbstractEncoder implements Int32Encoder {

  private static final int MIN_BLOCK_SIZE = 128;
  private static final int MIN_MINIBLOCK_SIZE = 8;

  private long[] value_buffer = new long[2 * MIN_BLOCK_SIZE + 1];
  private int position = 0;
  private long[] deltas = new long[2 * MIN_BLOCK_SIZE];
  private long min_delta = 0;
  private int [] reference_frame = new int[2 * MIN_BLOCK_SIZE];

  private ByteArrayWriter best_encoding = new ByteArrayWriter(128);
  private ByteArrayWriter current_encoding = new ByteArrayWriter(128);

  @Override
  public void append(final int i) {
    if (position % MIN_BLOCK_SIZE == 1 && position > 2 * MIN_BLOCK_SIZE) {
      tryFlushFirstBlocks();
    }
    appendAndGrowIfNecessary(i);
  }

  private void growValueBuffer() {
    long[] new_buffer = new long[value_buffer.length << 1];
    System.arraycopy(value_buffer, 0, new_buffer, 0, value_buffer.length);
    value_buffer = new_buffer;
  }

  private void appendAndGrowIfNecessary(final int i) {
    if (position == value_buffer.length) {
      growValueBuffer();
      appendAndGrowIfNecessary(i);
    } else {
      value_buffer[position] = i;
      position += 1;
    }
  }

  private ByteArrayWriter getBestMiniblockEncodingForBlock(final int start_position, final int end_position) {
    int num_values = end_position - start_position;
    int block_size = getBlockSize(num_values);
    int start_value = (int)value_buffer[start_position];
    computeDeltas(start_position, end_position, block_size);
    computeFrameOfReference(num_values);

    int miniblock_size = 8;

    best_encoding.reset();
    flushBlockWithNumMiniBlocks(best_encoding, miniblock_size, block_size, num_values, start_value);

    int min_size = best_encoding.size();

    miniblock_size <<= 1;
    while (miniblock_size <= MIN_BLOCK_SIZE) {
      current_encoding.reset();
      flushBlockWithNumMiniBlocks(current_encoding, miniblock_size, block_size, num_values, start_value);
      int current_size = current_encoding.size();
      if (current_size < min_size) {
        ByteArrayWriter tmp = best_encoding;
        best_encoding = current_encoding;
        current_encoding = tmp;
        min_size = current_size;
      }
      miniblock_size <<= 1;
    }

    if (block_size > MIN_BLOCK_SIZE) {
      current_encoding.reset();
      flushBlockWithNumMiniBlocks(current_encoding, block_size, block_size, num_values, start_value);
      if (current_encoding.size() < min_size) {
        best_encoding = current_encoding;
      }
    }

    return best_encoding;
  }

  private void flushBlockWithNumMiniBlocks(final ByteArrayWriter baw, final int miniBlockSize,
                                          final int block_size, final int num_values,
                                          final int start_value) {
    int num_miniblocks = block_size / miniBlockSize;
    int[] miniblock_bit_widths = new int[num_miniblocks];
    for (int j=0; j<num_miniblocks; ++j) {
      miniblock_bit_widths[j] = getMiniBlockBitWidth(j * miniBlockSize, miniBlockSize);
    }
    baw.writeUInt32(block_size);
    baw.writeUInt32(num_miniblocks);
    baw.writeUInt32(num_values);
    baw.writeSInt32(start_value);
    if (num_miniblocks > 0){
      baw.writeSInt64(min_delta);
      for (int j=0; j<num_miniblocks; ++j) {
        baw.writeByte((byte) (miniblock_bit_widths[j] & 0xff));
      }
      for (int j=0; j<num_miniblocks; ++j) {
        int num_remaining_values = num_values - 1 - j * miniBlockSize;
        int length = num_remaining_values < miniBlockSize ? num_remaining_values : miniBlockSize;
        baw.writePackedInts32(reference_frame, miniblock_bit_widths[j], j * miniBlockSize, length);
      }
    }
  }

  private int getMiniBlockBitWidth(final int miniBlockStartPosition, final int miniBlockSize) {
    int max_bit_width = 0;
    for (int j=miniBlockStartPosition; j<miniBlockStartPosition+miniBlockSize; ++j) {
      int bit_width = ByteArrayWriter.getBitWidth(reference_frame[j]);
      if (bit_width > max_bit_width) {
        max_bit_width = bit_width;
      }
    }
    return max_bit_width;
  }

  private void ensureDeltasBufferIsLargeEnough(final int size) {
    if (deltas.length < size) {
      deltas = new long[size];
    }
  }

  private void computeDeltas(final int start_position, final int end_position, final int block_size) {
    ensureDeltasBufferIsLargeEnough(block_size);
    min_delta = Long.MAX_VALUE;
    for (int j=start_position+1; j<end_position; ++j) {
      long delta = value_buffer[j] - value_buffer[j-1];
      deltas[j-1] = delta;
      if (delta < min_delta) {
        min_delta = delta;
      }
    }
  }

  private void prepareFrameOfReference(final int size) {
    if (reference_frame.length < size) {
      reference_frame = new int[size];
    } else {
      for (int i=0; i<size; i++) {
        reference_frame[i] = 0;
      }
    }
  }

  private void computeFrameOfReference(final int num_values) {
    prepareFrameOfReference(deltas.length);
    for (int j=0; j<num_values-1; ++j) {
      long relative_delta = deltas[j] - min_delta;
      reference_frame[j] = (int) relative_delta;
    }
  }

  private static int getBlockSize(final int num_values_in_block) {
    int ints_after_first = num_values_in_block - 1;
    if (ints_after_first == 0) {
      return 0;
    } else if (ints_after_first < MIN_BLOCK_SIZE) {
      int block_size = 8;
      while (ints_after_first > block_size && block_size < MIN_BLOCK_SIZE) {
        block_size <<= 1;
      }
      return block_size;
    } else {
      int block_size = MIN_BLOCK_SIZE;
      while (ints_after_first > block_size) {
        block_size += MIN_BLOCK_SIZE;
      }
      return block_size;
    }
  }

  private int getEndPositionOfLastFullBlock() {
    return ((position / MIN_BLOCK_SIZE) - 1) * MIN_BLOCK_SIZE + 1;
  }

  private int getSizeEncodedAsOneBlock() {
    return getBestMiniblockEncodingForBlock(0, position).size();
  }

  private int getSizeEncodedAsTwoBlocks() {
    int end_position_of_last_full_block = getEndPositionOfLastFullBlock();
    /* compute the blocks in this order so that best_encoding contains the serialized first block in we choose
       to flush it */
    int second_block_size = getBestMiniblockEncodingForBlock(end_position_of_last_full_block, position).size();
    int first_block_size = getBestMiniblockEncodingForBlock(0, end_position_of_last_full_block).size();
    return first_block_size + second_block_size;
  }

  private void tryFlushFirstBlocks() {
    if (position > MIN_BLOCK_SIZE + 1){
      int size_as_one_block = getSizeEncodedAsOneBlock();
      int size_as_two_blocks = getSizeEncodedAsTwoBlocks();
      if (size_as_two_blocks < size_as_one_block) {
        flushFirstBlocks();
      }
    }
  }

  private void flushFirstBlocks() {
    best_encoding.writeTo(byte_array_writer);
    int num_flushed_values = getEndPositionOfLastFullBlock();
    position -= num_flushed_values;
    for (int i=0; i<position; ++i) {
      value_buffer[i] = value_buffer[i+num_flushed_values];
    }
  }

  private void flushAllBlocks() {
    tryFlushFirstBlocks();
    if (position > 0) {
      ByteArrayWriter full_block_encoding = getBestMiniblockEncodingForBlock(0, position);
      full_block_encoding.writeTo(byte_array_writer);
    }
    position = 0;
  }

  @Override
  public void reset() {
    position = 0;
    super.reset();
  }

  @Override
  public void flush() {
    if (position > 0){
      flushAllBlocks();
    }
  }

}
