package dendrite.java;

import java.math.BigInteger;
import java.util.Arrays;

public class LongPackedDeltaEncoder extends AbstractEncoder {

  private static final int MAX_BLOCK_LENGTH = 128;
  private static final int MAX_MINIBLOCK_LENGTH = 32;
  private static final int MIN_MINIBLOCK_LENGTH = 8;

  private BigInteger[] value_buffer = new BigInteger[MAX_BLOCK_LENGTH + 1];
  private int position = 0;
  private BigInteger[] deltas = new BigInteger[MAX_BLOCK_LENGTH];
  private BigInteger min_delta;
  private long [] reference_frame = new long[MAX_BLOCK_LENGTH];
  private int num_encoded_values = 0;

  private ByteArrayWriter best_encoding = new ByteArrayWriter(128);
  private ByteArrayWriter current_encoding = new ByteArrayWriter(128);

  @Override
  public void encode(final Object o) {
    num_values += 1;
    if (position == MAX_BLOCK_LENGTH + 1) {
      flushBlock();
    }
    value_buffer[position] = BigInteger.valueOf((long)o);
    position += 1;
  }

  private ByteArrayWriter getBestMiniblockEncodingForBlock() {
    int block_length = getBlockLength(position);
    computeDeltas(block_length);
    computeFrameOfReference();

    int miniblock_length = MIN_MINIBLOCK_LENGTH;

    best_encoding.reset();
    flushBlockWithNumMiniBlocks(best_encoding, miniblock_length, block_length);

    int min_length = best_encoding.length();

    miniblock_length <<= 1;
    while (miniblock_length <= Math.min(MAX_BLOCK_LENGTH, block_length)) {
      current_encoding.reset();
      flushBlockWithNumMiniBlocks(current_encoding, miniblock_length, block_length);
      int current_length = current_encoding.length();
      if (current_length < min_length) {
        ByteArrayWriter tmp = best_encoding;
        best_encoding = current_encoding;
        current_encoding = tmp;
        min_length = current_length;
      }
      miniblock_length <<= 1;
    }

    return best_encoding;
  }

  private void flushBlockWithNumMiniBlocks(final ByteArrayWriter baw, final int mini_block_length,
                                          final int block_length) {
    int num_miniblocks = block_length / mini_block_length;
    long start_value = value_buffer[0].longValue();
    int[] miniblock_bit_widths = new int[num_miniblocks];
    for (int j=0; j<num_miniblocks; ++j) {
      miniblock_bit_widths[j] = getMiniBlockBitWidth(j * mini_block_length, mini_block_length);
    }
    baw.writeUInt(block_length);
    baw.writeUInt(num_miniblocks);
    baw.writeUInt(position);
    baw.writeSLong(start_value);
    if (num_miniblocks > 0){
      baw.writeSIntVLQ(min_delta);
      for (int j=0; j<num_miniblocks; ++j) {
        baw.writeByte((byte) (miniblock_bit_widths[j] & 0xff));
      }
      for (int j=0; j<num_miniblocks; ++j) {
        int num_remaining_values = position - 1 - j * mini_block_length;
        int length = num_remaining_values < mini_block_length ? num_remaining_values : mini_block_length;
        baw.writePackedInts64(reference_frame, miniblock_bit_widths[j], j * mini_block_length, length);
      }
    }
  }

  private int getMiniBlockBitWidth(final int miniBlockStartPosition, final int miniBlockLength) {
    int max_bit_width = 0;
    for (int j=miniBlockStartPosition; j<miniBlockStartPosition+miniBlockLength; ++j) {
      int bit_width = ByteArrayWriter.getBitWidth(reference_frame[j]);
      if (bit_width > max_bit_width) {
        max_bit_width = bit_width;
      }
    }
    return max_bit_width;
  }

  private static final BigInteger INFINITY = BigInteger.valueOf(Long.MAX_VALUE).shiftLeft(2);

  private void computeDeltas(final int block_length) {
    min_delta = INFINITY;
    for (int j=0; j<position-1; ++j) {
      BigInteger delta = value_buffer[j+1].subtract(value_buffer[j]);
      deltas[j] = delta;
      if (delta.compareTo(min_delta) == -1) {
        min_delta = delta;
      }
    }
  }

  private void computeFrameOfReference() {
    Arrays.fill(reference_frame, 0, position-1, 0);
    for (int j=0; j<position-1; ++j) {
      BigInteger relative_delta = deltas[j].subtract(min_delta);
      reference_frame[j] = relative_delta.longValue();
    }
  }

  private static int getBlockLength(final int num_values_in_block) {
    int ints_after_first = num_values_in_block - 1;
    if (ints_after_first == 0) {
      return 0;
    } else {
      int block_length = 8;
      while (ints_after_first > block_length && block_length < MAX_BLOCK_LENGTH) {
        block_length <<= 1;
      }
      return block_length;
    }
  }

  private void flushBlock() {
    if (position > 0) {
      ByteArrayWriter full_block_encoding = getBestMiniblockEncodingForBlock();
      full_block_encoding.flush(byte_array_writer);
      num_encoded_values += position;
    }
    position = 0;
  }

  @Override
  public void reset() {
    position = 0;
    num_encoded_values = 0;
    super.reset();
  }

  @Override
  public void finish() {
    if (position > 0){
      flushBlock();
    }
  }

  @Override
  public int estimatedLength() {
    if (num_encoded_values == 0) {
      return position; // very rough estimate when we haven't flushed anything yet
    }
    return super.estimatedLength() * (int)(1 + (double)position / (double)num_encoded_values);
  }

}
