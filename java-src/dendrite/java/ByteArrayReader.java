package dendrite.java;

public class ByteArrayReader {

  public final byte[] buffer;
  private int position = 0;

  public ByteArrayReader(final byte[] buf) {
    buffer = buf;
  }

  public ByteArrayReader(final byte[] buf, final int offset) {
    buffer = buf;
    position = offset;
  }

  public byte readByte() {
    byte b = buffer[position];
    position += 1;
    return b;
  }

  public int readFixedInt32() {
    final int i1 =  (int) readByte() & 0xff;
    final int i2 = ((int) readByte() & 0xff) <<  8;
    final int i3 = ((int) readByte() & 0xff) << 16;
    final int i4 = ((int) readByte() & 0xff) << 24;
    return i4 | i3 | i2 | i1;
  }

  public int readUInt32() {
    int shift = 0;
    int result = 0;
    while (shift < 32) {
      final byte b = readByte();
      result |= (int)(b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        return result;
      }
      shift += 7;
    }
    throw new IllegalStateException("Failed to parse UInt32");
  }

  private static int decodeZigZag32(final int i) {
    return (i >>> 1) ^ -(i & 1);
  }

  public int readSInt32() {
    return decodeZigZag32(readUInt32());
  }

  public long readFixedInt64() {
    final long l1 =  (long) readByte() & 0xff;
    final long l2 = ((long) readByte() & 0xff) << 8;
    final long l3 = ((long) readByte() & 0xff) << 16;
    final long l4 = ((long) readByte() & 0xff) << 24;
    final long l5 = ((long) readByte() & 0xff) << 32;
    final long l6 = ((long) readByte() & 0xff) << 40;
    final long l7 = ((long) readByte() & 0xff) << 48;
    final long l8 = ((long) readByte() & 0xff) << 56;
    return l8 | l7 | l6 | l5 | l4 | l3 | l2 | l1;
  }

  public long readUInt64() {
    int shift = 0;
    long result = 0;
    while (shift < 64) {
      final byte b = readByte();
      result |= (long)(b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        return result;
      }
      shift += 7;
    }
    throw new IllegalStateException("Failed to parse UInt64");
  }

  private static long decodeZigZag64(final long l) {
    return (l >>> 1) ^ -(l & 1);
  }

  public long readSInt64() {
    return decodeZigZag64(readUInt64());
  }

  public void readPackedBooleans(final boolean[] booleanOctuplet) {
    byte b = readByte();
    booleanOctuplet[0] = ((b & 128) > 0);
    booleanOctuplet[1] = ((b &  64) > 0);
    booleanOctuplet[2] = ((b &  32) > 0);
    booleanOctuplet[3] = ((b &  16) > 0);
    booleanOctuplet[4] = ((b &   8) > 0);
    booleanOctuplet[5] = ((b &   4) > 0);
    booleanOctuplet[6] = ((b &   2) > 0);
    booleanOctuplet[7] = ((b &   1) > 0);
  }

  public float readFloat() {
    return Float.intBitsToFloat(readFixedInt32());
  }

  public double readDouble() {
    return Double.longBitsToDouble(readFixedInt64());
  }

  public void readByteArray(final byte[] bytes) {
    readByteArray(bytes, 0, bytes.length);
  }

  public void readByteArray(final byte[] bytes, final int length) {
    readByteArray(bytes, 0, length);
  }

  public void readByteArray(final byte[] bytes, final int offset, final int length) {
    System.arraycopy(buffer, position, bytes, offset, length);
    position += length;
  }

  public void readPackedInts32(final int[] ints, final int width, final int length) {
    readPackedInts32(ints, width, 0, length);
  }

  public int readPackedInt32(final int width) {
    final int mask = ~((-1) << width);
    int remaining_width = width;
    int i = 0;
    while (remaining_width > 0) {
      i = (i << 8) | ((int) readByte() & 0xff);
      remaining_width -= 8;
    }
    return i;
  }

  public void readPackedInts32(final int[] ints, final int width, final int offset, final int length) {
    final int mask = ~((-1) << width);
    int current_bytes = 0;
    int available_bits = 0;
    for (int i=0; i<length; ++i) {
      while (available_bits < width) {
        current_bytes |= ((int) readByte() & 0xff) << available_bits;
        available_bits += 8;
      }
      ints[i] = (current_bytes & mask);
      current_bytes >>>= width;
      available_bits -= width;
    }
  }

  public void readPackedInts64(final long[] longs, final int width, final int length) {
    readPackedInts64(longs, width, 0, length);
  }

  public void readPackedInts64(final long[] longs, final int width, final int offset, final int length) {
    final long mask = ~(((long)(-1)) << width);
    long current_bytes = 0;
    int available_bits = 0;
    for (int i=0; i<length; ++i) {
      while (available_bits < width) {
        current_bytes |= ((int) readByte() & 0xff) << available_bits;
        available_bits += 8;
      }
      longs[i] = (current_bytes & mask);
      current_bytes >>>= width;
      available_bits -= width;
    }
  }

}
