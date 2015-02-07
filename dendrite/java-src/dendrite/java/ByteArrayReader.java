/**
* Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
*
* The use and distribution terms for this software are covered by the
* Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
* which can be found in the file epl-v10.txt at the root of this distribution.
* By using this software in any fashion, you are agreeing to be bound by
* the terms of this license.
*
* You must not remove this notice, or any other, from this software.
*/

package dendrite.java;

import java.nio.ByteBuffer;
import java.math.BigInteger;

public class ByteArrayReader {

  public final byte[] buffer;
  public int position = 0;

  public ByteArrayReader(final byte[] buffer) {
    this.buffer = buffer;
  }

  public ByteArrayReader(final byte[] buffer, final int offset) {
    this.buffer = buffer;
    position = offset;
  }

  public ByteArrayReader(final ByteBuffer byteBuffer) {
    if (byteBuffer.hasArray()) {
      buffer = byteBuffer.array();
      position = byteBuffer.position();
    } else {
      int length = byteBuffer.limit() - byteBuffer.position();
      buffer = new byte[length];
      byteBuffer.slice().get(buffer);
    }
  }

  public byte readByte() {
    byte b = buffer[position];
    position += 1;
    return b;
  }

  public int readFixedInt() {
    final int i1 =  (int) readByte() & 0xff;
    final int i2 = ((int) readByte() & 0xff) <<  8;
    final int i3 = ((int) readByte() & 0xff) << 16;
    final int i4 = ((int) readByte() & 0xff) << 24;
    return i4 | i3 | i2 | i1;
  }

  public int readUInt() {
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
    throw new IllegalStateException("Failed to parse UInt");
  }

  private static int decodeZigZag32(final int i) {
    return (i >>> 1) ^ -(i & 1);
  }

  public int readSInt() {
    return decodeZigZag32(readUInt());
  }

  public long readFixedLong() {
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

  public long readULong() {
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
    throw new IllegalStateException("Failed to parse ULong");
  }

  private static long decodeZigZag64(final long l) {
    return (l >>> 1) ^ -(l & 1);
  }

  public long readSLong() {
    return decodeZigZag64(readULong());
  }

  public BigInteger readBigInt() {
    int length = readUInt();
    byte[] intAsBytes = new byte[length];
    readByteArray(intAsBytes, 0, length);
    return new BigInteger(intAsBytes);
  }

  public BigInteger readUIntVLQ() {
    ByteArrayWriter baw = new ByteArrayWriter(10);
    int byteBuffer = 0;
    int shift = 0;
    while (true) {
      int currentByte = (int)readByte();
      byteBuffer |= (currentByte & 0x7f) << shift;
      shift += 7;
      while (shift >= 8) {
        baw.writeByte((byte)byteBuffer);
        byteBuffer >>>= 8;
        shift -= 8;
      }
      if ((currentByte & 0x80) == 0) {
        baw.writeByte((byte)byteBuffer);
        byte[] bytesLittleEndian = baw.buffer;
        int length = baw.length();
        byte[] bytesBigEndian = new byte[length];
        for(int i=0; i<length; ++i) {
          bytesBigEndian[i] = bytesLittleEndian[length-i-1];
        }
        return new BigInteger(bytesBigEndian);
      }
    }
  }

  public static BigInteger decodeZigZag(final BigInteger bi) {
    boolean isPositive = !bi.testBit(0);
    BigInteger positiveBigInteger = bi.shiftRight(1);
    return isPositive? positiveBigInteger : positiveBigInteger.negate();
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
    return Float.intBitsToFloat(readFixedInt());
  }

  public double readDouble() {
    return Double.longBitsToDouble(readFixedLong());
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

  public void readBytes(ByteArrayWriter baw, int n) {
    baw.writeByteArray(buffer, position, n);
    position += n;
  }

  public int readPackedInt(final int width) {
    final int mask = (width == 32)? -1 : ~((-1) << width);
    int i = 0;
    int readBits = 0;
    while (readBits < width) {
      i |= ((int) readByte() & 0xff) << readBits;
      readBits += 8;
    }
    return i & mask;
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
    } else {
      for (int i=0; i<length; ++i) {
        ints[i] = 0;
      }
    }
  }

  private void readPackedInts32Under24bits(final int[] ints, final int width,
                                           final int offset, final int length) {
    final int mask = ~((-1) << width);
    int currentBytes = 0;
    int availableBits = 0;
    for (int i=0; i<length; ++i) {
      while (availableBits < width) {
        currentBytes |= ((int) readByte() & 0xff) << availableBits;
        availableBits += 8;
      }
      ints[i] = (currentBytes & mask);
      currentBytes >>>= width;
      availableBits -= width;
    }
  }

  private void readPackedInts32Over24bits(final int[] ints, final int width,
                                         final int offset, final int length) {
    final long mask = ~((long)-1 << width);
    long currentBytes = 0;
    int availableBits = 0;
    for (int i=0; i<length; ++i) {
      while (availableBits < width) {
        currentBytes |= ((long) readByte() & 0xff) << availableBits;
        availableBits += 8;
      }
      ints[i] = (int)(currentBytes & mask);
      currentBytes >>>= width;
      availableBits -= width;
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
    long currentBytes = 0;
    int availableBits = 0;
    for (int i=0; i<length; ++i) {
      while (availableBits < width) {
        currentBytes |= ((long) readByte() & 0xff) << availableBits;
        availableBits += 8;
      }
      longs[i] = (currentBytes & mask);
      currentBytes >>>= width;
      availableBits -= width;
    }
  }

  private void readPackedInts64Over56bits(final long[] longs, final int width,
                                          final int offset, final int length) {
    final long mask = width==64? (long)-1 : ~(((long)(-1)) << width);
    long currentBytesLo = 0;
    long currentBytesHi = 0;
    int availableBits = 0;
    for (int i=0; i<length; ++i) {
      while (availableBits < width) {
        long b = (long) readByte() & 0xff;
        currentBytesLo |= b << availableBits;
        currentBytesHi = b >>> (64 - availableBits);
        availableBits += 8;
      }
      longs[i] = (currentBytesLo & mask);
      currentBytesLo = width==64? 0 : (currentBytesHi << (64 - width)) | (currentBytesLo >>> width);
      availableBits -= width;
    }
  }

  public ByteArrayReader slice() {
    return new ByteArrayReader(buffer, position);
  }

  public ByteArrayReader sliceAhead(int offset) {
    return new ByteArrayReader(buffer, position + offset);
  }
}
