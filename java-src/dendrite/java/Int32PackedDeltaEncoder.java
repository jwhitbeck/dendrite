package dendrite.java;

public class Int32PackedDeltaEncoder extends AbstractEncoder implements Int32Encoder {

  public final long[] block_buffer = new long[129];
  public final long[] deltas = new long[128];
  public final int [] reference_frame = new int[128];
  private int block_position = 0;
  private long min_delta;
  private int block_size;
  private ByteArrayWriter best_encoding = new ByteArrayWriter(128);
  private ByteArrayWriter current_encoding = new ByteArrayWriter(128);

  @Override
  public void append(int i) {
    if (block_position == 129) {
      flushBlock();
    }
    block_buffer[block_position] = i;
    block_position += 1;
  }

  private void flushBlock() {

    setBlockSize();
    computeDeltas();
    computeFrameOfReference();

    int miniblock_size = 8;
    int min_size = Integer.MAX_VALUE;

    best_encoding.reset();
    flushBlockWithNumMiniBlocks(best_encoding, miniblock_size);

    miniblock_size <<= 1;
    while (miniblock_size <= block_size) {
      current_encoding.reset();
      flushBlockWithNumMiniBlocks(current_encoding, miniblock_size);
      int current_size = current_encoding.size();
      if (current_size < min_size) {
        ByteArrayWriter tmp = best_encoding;
        best_encoding = current_encoding;
        current_encoding = tmp;
        min_size = current_size;
      }
      miniblock_size <<= 1;
    }
    best_encoding.writeTo(byte_array_writer);
    block_position = 0;
  }

  private void flushBlockWithNumMiniBlocks(final ByteArrayWriter baw, final int miniBlockSize) {
    int num_miniblocks = block_size / miniBlockSize;
    int[] miniblock_bit_widths = new int[num_miniblocks];
    for (int j=0; j<num_miniblocks; ++j) {
      miniblock_bit_widths[j] = getMiniBlockBitWidth(j * miniBlockSize, miniBlockSize);
    }
    baw.writeUInt32(block_size);
    baw.writeUInt32(num_miniblocks);
    baw.writeUInt32(block_position);
    baw.writeSInt32((int)block_buffer[0]);
    baw.writeSInt64(min_delta);
    for (int j=0; j<num_miniblocks; ++j) {
      baw.writeByte((byte) (miniblock_bit_widths[j] & 0xff));
    }
    for (int j=0; j<num_miniblocks; ++j) {
      int num_remaining_values = block_position - 1 - j * miniBlockSize;
      int length = num_remaining_values < miniBlockSize ? num_remaining_values : miniBlockSize;
      baw.writePackedInts32(reference_frame, miniblock_bit_widths[j], j * miniBlockSize, length);
    }
  }

  private int getMiniBlockBitWidth(final int miniBlockStartPosition, final int miniBlockSize) {
    long max_relative_delta = 0;
    for (int j=miniBlockStartPosition; j<miniBlockStartPosition+miniBlockSize; ++j) {
      long relative_delta = reference_frame[j] & 0xffffffffL;
      if (relative_delta > max_relative_delta) {
        max_relative_delta = relative_delta;
      }
    }
    return ByteArrayWriter.getBitWidth(max_relative_delta);
  }

  private void computeDeltas() {
    min_delta = Long.MAX_VALUE;
    for (int j=1; j<block_position; ++j) {
      long delta = block_buffer[j] - block_buffer[j-1];
      deltas[j-1] = delta;
      if (delta < min_delta) {
        min_delta = delta;
      }
    }
  }

  private void computeFrameOfReference() {
    for (int j=1; j<block_position; ++j) {
      long relative_delta = deltas[j-1] - min_delta;
      reference_frame[j-1] = (int) relative_delta;
    }
  }

  private void setBlockSize() {
    block_size = getBlockSize();
  }

  private int getBlockSize() {
    int ints_after_first = block_position - 1;
    if (ints_after_first == 0) return 0;
    if (ints_after_first <= 8) return 8;
    if (ints_after_first <= 16) return 16;
    if (ints_after_first <= 32) return 32;
    if (ints_after_first <= 64) return 64;
    return 128;
  }

  @Override
  public void reset() {
    block_position = 0;
    super.reset();
  }

  @Override
  public void flush() {
    flushBlock();
    super.flush();
  }

}
