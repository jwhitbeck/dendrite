package dendrite.java;

import java.math.BigInteger;

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
      result |= (int)(b & 0x7f) << shift;
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
      result |= (long)(b & 0x7f) << shift;
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

  public BigInteger readBigInt() {
    int length = readUInt32();
    byte[] int_as_bytes = new byte[length];
    readByteArray(int_as_bytes, 0, length);
    return new BigInteger(int_as_bytes);
  }

  public BigInteger readUIntVLQ() {
    ByteArrayWriter baw = new ByteArrayWriter(10);
    int byte_buffer = 0;
    int shift = 0;
    while (true) {
      int current_byte = (int)readByte();
      byte_buffer |= (current_byte & 0x7f) << shift;
      shift += 7;
      while (shift >= 8) {
        baw.writeByte((byte)byte_buffer);
        byte_buffer >>>= 8;
        shift -= 8;
      }
      if ((current_byte & 0x80) == 0) {
        baw.writeByte((byte)byte_buffer);
        byte[] bytes_little_endian = baw.buffer;
        int length = baw.size();
        byte[] bytes_big_endian = new byte[length];
        for(int i=0; i<length; ++i) {
          bytes_big_endian[i] = bytes_little_endian[length-i-1];
        }
        return new BigInteger(bytes_big_endian);
      }
    }
  }

  public static BigInteger decodeZigZag(final BigInteger bi) {
    boolean is_positive = !bi.testBit(0);
    BigInteger positive_bi = bi.shiftRight(1);
    return is_positive? positive_bi : positive_bi.negate();
  }

  public BigInteger readSIntVLQ() {
    return decodeZigZag(readUIntVLQ());
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

  public void readPackedInts32(final int[] ints, final int width, final int length) {
    readPackedInts32(ints, width, 0, length);
  }

  public void readPackedInts32(final int[] ints, final int width, final int offset, final int length) {
    if (width > 0) {
      if (width < 25) {
        readPackedInts32Under24bits(ints, width, offset, length);
      } else {
        readPackedInts32Over24bits(ints, width, offset, length);
      }
    }
  }

  private void readPackedInts32Under24bits(final int[] ints, final int width,
                                           final int offset, final int length) {
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

  private void readPackedInts32Over24bits(final int[] ints, final int width,
                                         final int offset, final int length) {
    final long mask = ~((long)-1 << width);
    long current_bytes = 0;
    int available_bits = 0;
    for (int i=0; i<length; ++i) {
      while (available_bits < width) {
        current_bytes |= ((long) readByte() & 0xff) << available_bits;
        available_bits += 8;
      }
      ints[i] = (int)(current_bytes & mask);
      current_bytes >>>= width;
      available_bits -= width;
    }
  }


  public void readPackedInts64(final long[] longs, final int width, final int length) {
    readPackedInts64(longs, width, 0, length);
  }

  public void readPackedInts64(final long[] longs, final int width, final int offset, final int length) {
    if (width > 0) {
      if (width < 57) {
        readPackedInts64Under56bits(longs, width, offset, length);
      } else {
        readPackedInts64Over56bits(longs, width, offset, length);
      }
    } else {
      for (int i=0; i<length; ++i) {
        longs[i] = 0;
      }
    }
  }

  private void readPackedInts64Under56bits(final long[] longs, final int width,
                                           final int offset, final int length) {
    final long mask = ~(((long)(-1)) << width);
    long current_bytes = 0;
    int available_bits = 0;
    for (int i=0; i<length; ++i) {
      while (available_bits < width) {
        current_bytes |= ((long) readByte() & 0xff) << available_bits;
        available_bits += 8;
      }
      longs[i] = (current_bytes & mask);
      current_bytes >>>= width;
      available_bits -= width;
    }
  }

  private void readPackedInts64Over56bits(final long[] longs, final int width,
                                          final int offset, final int length) {
    final long mask = width==64? (long)-1 : ~(((long)(-1)) << width);
    long current_bytes_lo = 0;
    long current_bytes_hi = 0;
    int available_bits = 0;
    for (int i=0; i<length; ++i) {
      while (available_bits < width) {
        long b = (long) readByte() & 0xff;
        current_bytes_lo |= b << available_bits;
        current_bytes_hi = b >>> (64 - available_bits);
        available_bits += 8;
      }
      longs[i] = (current_bytes_lo & mask);
      current_bytes_lo = width==64? 0 : (current_bytes_hi << (64 - width)) | (current_bytes_lo >>> width);
      available_bits -= width;
    }
  }
}
