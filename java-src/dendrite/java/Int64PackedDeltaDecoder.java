package dendrite.java;

import java.math.BigInteger;

public class Int64PackedDeltaDecoder implements Int64Decoder {

  private final ByteArrayReader byte_array_reader;
  private final long[] miniblock_buffer = new long[128];
  private int miniblock_position = 0;
  private int miniblock_size = 0;
  private int current_miniblock_index = 0;
  private int num_miniblocks = 0;
  private final int[] miniblock_bit_widths = new int[32];
  private int remaining_values_in_block = 0;
  private int block_size = 0;
  private BigInteger block_min_delta = BigInteger.ZERO;
  private BigInteger block_current_value = BigInteger.ZERO;

  public Int64PackedDeltaDecoder(final ByteArrayReader baw) {
    byte_array_reader = baw;
  }

  @Override
  public long next() {
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
      return block_current_value.longValue();
    } else { // no more values in block, load next block
      initNextBlock();
      return next();
    }
  }

  public static BigInteger valueOfUnsignedLong(long l) {
    if (l >= 0) {
      return BigInteger.valueOf(l);
    } else {
      return BigInteger.valueOf(l & 0x7fffffffffffffffL).or(BigInteger.ZERO.flipBit(63));
    }
  }

  private void setCurrentValueFromMiniBlockBuffer() {
    BigInteger next_relative_delta = valueOfUnsignedLong(miniblock_buffer[miniblock_position]);
    BigInteger delta = next_relative_delta.add(block_min_delta);
    block_current_value = block_current_value.add(delta);
    miniblock_position += 1;
  }

  private void initNextMiniBlock() {
    current_miniblock_index += 1;
    int width = miniblock_bit_widths[current_miniblock_index];
    int length = remaining_values_in_block < miniblock_size? remaining_values_in_block : miniblock_size;
    byte_array_reader.readPackedInts64(miniblock_buffer, width, length);
    miniblock_position = 0;
  }

  private void initNextBlock() {
    block_size = byte_array_reader.readUInt32();
    num_miniblocks = byte_array_reader.readUInt32();
    miniblock_size = block_size / num_miniblocks;
    remaining_values_in_block = byte_array_reader.readUInt32();
    miniblock_position = -1;
    current_miniblock_index = -1;
    block_current_value = BigInteger.valueOf(byte_array_reader.readSInt64());
    block_min_delta = byte_array_reader.readSIntVLQ();
    for (int i=0; i<num_miniblocks; ++i) {
      miniblock_bit_widths[i] = (int)byte_array_reader.readByte() & 0xff;
    }
  }

}
