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

public final class Bytes {

  private static int DEFAULT_BUFFER_LENGTH = 1024;

  public static void writeFixedInt(final MemoryOutputStream os, final int i) {
    os.write(i);
    os.write(i >>  8);
    os.write(i >> 16);
    os.write(i >> 24);
  }

  public static int readFixedInt(final ByteBuffer bb) {
    final int i1 =  (int) bb.get() & 0xff;
    final int i2 = ((int) bb.get() & 0xff) <<  8;
    final int i3 = ((int) bb.get() & 0xff) << 16;
    final int i4 = ((int) bb.get() & 0xff) << 24;
    return i4 | i3 | i2 | i1;
  }

  public static int getNumUIntBytes(final int i) {
    int numBytes = 0;
    int v = i;
    do {
      numBytes += 1;
      v >>>= 7;
    } while (v != 0);
    return numBytes;
  }

  public static void writeUInt(final MemoryOutputStream os, final int i) {
    int value = i;
    while (true) {
      if ((value & ~0x7f) == 0) {
        os.write(value);
        return;
      } else {
        os.write((value & 0x7f) | 0x80);
        value >>>= 7;
      }
    }
  }

  public static int readUInt(final ByteBuffer bb) {
    int shift = 0;
    int result = 0;
    while (shift < 32) {
      final byte b = bb.get();
      result |= (int)(b & 0x7f) << shift;
      if ((b & 0x80) == 0) {
        return result;
      }
      shift += 7;
    }
    throw new IllegalStateException("Failed to parse UInt");
  }

  public static int encodeZigZag32(final int i) {
    return (i << 1) ^ (i >> 31);
  }

  public static int decodeZigZag32(final int i) {
    return (i >>> 1) ^ -(i & 1);
  }

  public static void writeSInt(final MemoryOutputStream os, final int i) {
    writeUInt(os, encodeZigZag32(i));
  }

  public static int readSInt(final ByteBuffer bb) {
    return decodeZigZag32(readUInt(bb));
  }

  public static void writeFixedLong(final MemoryOutputStream os, final long l) {
    os.write((int) l);
    os.write((int)(l >>  8));
    os.write((int)(l >> 16));
    os.write((int)(l >> 24));
    os.write((int)(l >> 32));
    os.write((int)(l >> 40));
    os.write((int)(l >> 48));
    os.write((int)(l >> 56));
  }

  public static long readFixedLong(final ByteBuffer bb) {
    final long l1 =  (long) bb.get() & 0xff;
    final long l2 = ((long) bb.get() & 0xff) << 8;
    final long l3 = ((long) bb.get() & 0xff) << 16;
    final long l4 = ((long) bb.get() & 0xff) << 24;
    final long l5 = ((long) bb.get() & 0xff) << 32;
    final long l6 = ((long) bb.get() & 0xff) << 40;
    final long l7 = ((long) bb.get() & 0xff) << 48;
    final long l8 = ((long) bb.get() & 0xff) << 56;
    return l8 | l7 | l6 | l5 | l4 | l3 | l2 | l1;
  }

  public static int getNumULongBytes(final long i) {
    int numBytes = 0;
    long v = i;
    do {
      numBytes += 1;
      v >>>= 7;
    } while (v != 0);
    return numBytes;
  }

  public static void writeULong(final MemoryOutputStream os, final long l) {
    long value = l;
    while (true) {
      if ((value & ~0x7fL) == 0) {
        os.write((int)value);
        return;
      } else {
        os.write((int)((value & 0x7f) | 0x80));
        value >>>= 7;
      }
    }
  }

  public static long readULong(final ByteBuffer bb) {
    int shift = 0;
    long result = 0;
    while (shift < 64) {
      final byte b = bb.get();
      result |= (long)(b & 0x7f) << shift;
      if ((b & 0x80) == 0) {
        return result;
      }
      shift += 7;
    }
    throw new IllegalStateException("Failed to parse ULong");
  }

  public static long encodeZigZag64(final long l) {
    return (l << 1) ^ (l >> 63);
  }

  public static long decodeZigZag64(final long l) {
    return (l >>> 1) ^ -(l & 1);
  }

  public static void writeSLong(final MemoryOutputStream os, final long l) {
    writeULong(os, encodeZigZag64(l));
  }

  public static long readSLong(final ByteBuffer bb) {
    return decodeZigZag64(readULong(bb));
  }

  public static void writeBigInt(final MemoryOutputStream os, final BigInteger bi) {
    byte[] intAsBytes = bi.toByteArray();
    writeUInt(os, intAsBytes.length);
    os.write(intAsBytes);
  }

  public static BigInteger readBigInt(final ByteBuffer bb) {
    int length = readUInt(bb);
    byte[] intAsBytes = new byte[length];
    bb.get(intAsBytes, 0, length);
    return new BigInteger(intAsBytes);
  }

  public static void writeUIntVLQ(final MemoryOutputStream os, final BigInteger bi) {
    byte[] intAsBytes = bi.toByteArray();
    int numBytes = intAsBytes.length;
    int byteBuffer = 0;
    int shift = 0;
    for (int i=numBytes-1; i>=0; --i) {
      int value = intAsBytes[i] & 0xff;
      int bitWidth = i==0? getBitWidth(value) : 8;
      byteBuffer |= value << shift;
      shift += bitWidth;
      while (shift >= 7) {
        os.write((byteBuffer & 0x7f) | 0x80);
        byteBuffer >>>= 7;
        shift -= 7;
      }
    }
    os.write(byteBuffer);
  }

  public static BigInteger readUIntVLQ(final ByteBuffer bb) {
    MemoryOutputStream mos = new MemoryOutputStream(10);
    int byteBuffer = 0;
    int shift = 0;
    while (true) {
      int currentByte = (int)bb.get();
      byteBuffer |= (currentByte & 0x7f) << shift;
      shift += 7;
      while (shift >= 8) {
        mos.write(byteBuffer);
        byteBuffer >>>= 8;
        shift -= 8;
      }
      if ((currentByte & 0x80) == 0) {
        mos.write(byteBuffer);
        ByteBuffer bytesLittleEndian = mos.byteBuffer();
        int length = mos.length();
        byte[] bytesBigEndian = new byte[length];
        for(int i=0; i<length; ++i) {
          bytesBigEndian[i] = bytesLittleEndian.get(length-i-1);
        }
        return new BigInteger(bytesBigEndian);
      }
    }
  }

  private static BigInteger encodeZigZag(final BigInteger bi) {
    if ( bi.signum() < 0 ){
      return bi.negate().shiftLeft(1).add(BigInteger.ONE);
    } else {
      return bi.shiftLeft(1);
    }
  }

  public static BigInteger decodeZigZag(final BigInteger bi) {
    boolean isPositive = !bi.testBit(0);
    BigInteger positiveBigInteger = bi.shiftRight(1);
    return isPositive? positiveBigInteger : positiveBigInteger.negate();
  }

  public static void writeSIntVLQ(final MemoryOutputStream os, final BigInteger bi) {
    writeUIntVLQ(os, encodeZigZag(bi));
  }

  public static BigInteger readSIntVLQ(final ByteBuffer bb) {
    return decodeZigZag(readUIntVLQ(bb));
  }

  public static void writePackedBooleans(final MemoryOutputStream os,
                                         final boolean[] booleanOctuplet) {
    int b = 0;
    for (int i=0; i<8; ++i){
      b = (b << 1) | (booleanOctuplet[i]? 1 : 0);
    }
    os.write(b);
  }

  public static void readPackedBooleans(final ByteBuffer bb, final boolean[] booleanOctuplet) {
    byte b = bb.get();
    booleanOctuplet[0] = ((b & 128) > 0);
    booleanOctuplet[1] = ((b &  64) > 0);
    booleanOctuplet[2] = ((b &  32) > 0);
    booleanOctuplet[3] = ((b &  16) > 0);
    booleanOctuplet[4] = ((b &   8) > 0);
    booleanOctuplet[5] = ((b &   4) > 0);
    booleanOctuplet[6] = ((b &   2) > 0);
    booleanOctuplet[7] = ((b &   1) > 0);
  }

  public static void writeFloat(final MemoryOutputStream os, final float f) {
    writeFixedInt(os, Float.floatToRawIntBits(f));
  }

  public static float readFloat(final ByteBuffer bb) {
    return Float.intBitsToFloat(readFixedInt(bb));
  }

  public static void writeDouble(final MemoryOutputStream os, final double d) {
    writeFixedLong(os, Double.doubleToRawLongBits(d));
  }

  public static double readDouble(final ByteBuffer bb) {
    return Double.longBitsToDouble(readFixedLong(bb));
  }

  public static int getBitWidth(int i) {
    return 32 - Integer.numberOfLeadingZeros(i);
  }

  public static int getBitWidth(long l) {
    return 64 - Long.numberOfLeadingZeros(l);
  }

  public static void writePackedInt(final MemoryOutputStream os, final int i, final int width) {
    final int mask = (width == 32)? -1 : ~((-1) << width);
    int currentByte = i & mask;
    int remainingWidth = width;
    while (remainingWidth > 0) {
      os.write(currentByte);
      currentByte >>>= 8;
      remainingWidth -= 8;
    }
  }

  public static int readPackedInt(final ByteBuffer bb, final int width) {
    final int mask = (width == 32)? -1 : ~((-1) << width);
    int i = 0;
    int readBits = 0;
    while (readBits < width) {
      i |= ((int) bb.get() & 0xff) << readBits;
      readBits += 8;
    }
    return i & mask;
  }

  public static void writePackedInts32(final MemoryOutputStream os, final int[] ints, final int width,
                                       final int length) {
    writePackedInts32(os, ints, width, 0, length);
  }

  public static void readPackedInts32(final ByteBuffer bb, final int[] ints, final int width, final int length) {
    readPackedInts32(bb, ints, width, 0, length);
  }

  public static void writePackedInts32(final MemoryOutputStream os, final int[] ints, final int width,
                                       final int offset, final int length) {
    if (width > 0) {
      if (width < 25) {
        writePackedInts32Under24Bits(os, ints, width, offset, length);
      } else {
        writePackedInts32Over24Bits(os, ints, width, offset, length);
      }
    }
  }

  public static void readPackedInts32(final ByteBuffer bb, final int[] ints, final int width,
                                      final int offset, final int length) {
    if (width > 0) {
      if (width < 25) {
        readPackedInts32Under24bits(bb, ints, width, offset, length);
      } else {
        readPackedInts32Over24bits(bb, ints, width, offset, length);
      }
    } else {
      for (int i=0; i<length; ++i) {
        ints[i] = 0;
      }
    }
  }

  private static void writePackedInts32Under24Bits(final MemoryOutputStream os, final int[] ints,
                                                   final int width, final int offset, final int length) {
    final int mask = ~((-1) << width);
    int shift = 0;
    int currentByte = 0;
    for (int i=offset; i<offset+length; ++i) {
      currentByte |= (ints[i] & mask) << shift;
      shift += width;
      while (shift >= 8) {
        os.write(currentByte);
        currentByte >>>= 8;
        shift -= 8;
      }
    }
    if (shift > 0) {
      os.write(currentByte);
    }
  }

  private static void readPackedInts32Under24bits(final ByteBuffer bb, final int[] ints, final int width,
                                                  final int offset, final int length) {
    final int mask = ~((-1) << width);
    int currentBytes = 0;
    int availableBits = 0;
    for (int i=0; i<length; ++i) {
      while (availableBits < width) {
        currentBytes |= ((int) bb.get() & 0xff) << availableBits;
        availableBits += 8;
      }
      ints[i] = (currentBytes & mask);
      currentBytes >>>= width;
      availableBits -= width;
    }
  }

  private static void writePackedInts32Over24Bits(final MemoryOutputStream os, final int[] ints,
                                                  final int width, final int offset, final int length) {
    final long mask = ~((long)-1 << width);
    int shift = 0;
    long currentByte = 0;
    for (int i=offset; i<offset+length; ++i) {
      currentByte |= ((long)ints[i] & mask) << shift;
      shift += width;
      while (shift >= 8) {
        os.write((int)currentByte);
        currentByte >>>= 8;
        shift -= 8;
      }
    }
    if (shift > 0) {
      os.write((int)currentByte);
    }
  }

  private static void readPackedInts32Over24bits(final ByteBuffer bb, final int[] ints, final int width,
                                                 final int offset, final int length) {
    final long mask = ~((long)-1 << width);
    long currentBytes = 0;
    int availableBits = 0;
    for (int i=0; i<length; ++i) {
      while (availableBits < width) {
        currentBytes |= ((long) bb.get() & 0xff) << availableBits;
        availableBits += 8;
      }
      ints[i] = (int)(currentBytes & mask);
      currentBytes >>>= width;
      availableBits -= width;
    }
  }

  public static void writePackedInts64(final MemoryOutputStream os, final long[] longs, final int width,
                                       final int length) {
    writePackedInts64(os, longs, width, 0, length);
  }


  public static void readPackedInts64(final ByteBuffer bb, final long[] longs, final int width,
                                      final int length) {
    readPackedInts64(bb, longs, width, 0, length);
  }

  public static void writePackedInts64(final MemoryOutputStream os, final long[] longs, final int width,
                                       final int offset, final int length) {
    if (width > 0) {
      if (width < 57) {
        writePackedInts64Under56bits(os, longs, width, offset, length);
      } else {
        writePackedInts64Over56bits(os, longs, width, offset, length);
      }
    }
  }

  public static void readPackedInts64(final ByteBuffer bb, final long[] longs, final int width,
                                      final int offset, final int length) {
    if (width > 0) {
      if (width < 57) {
        readPackedInts64Under56bits(bb, longs, width, offset, length);
      } else {
        readPackedInts64Over56bits(bb, longs, width, offset, length);
      }
    } else {
      for (int i=0; i<length; ++i) {
        longs[i] = 0;
      }
    }
  }

  private static void writePackedInts64Under56bits(final MemoryOutputStream os, final long[] longs,
                                                   final int width, final int offset, final int length) {
    final long mask = ~(((long)-1) << width);
    int shift = 0;
    long currentByte = 0;
    for (int i=offset; i<offset+length; ++i) {
      currentByte |= (longs[i] & mask) << shift;
      shift += width;
      while (shift >= 8) {
        os.write((int)currentByte);
        currentByte >>>= 8;
        shift -= 8;
      }
    }
    if (shift > 0) {
      os.write((int)currentByte);
    }
  }

  private static void readPackedInts64Under56bits(final ByteBuffer bb, final long[] longs, final int width,
                                                  final int offset, final int length) {
    final long mask = ~(((long)(-1)) << width);
    long currentBytes = 0;
    int availableBits = 0;
    for (int i=0; i<length; ++i) {
      while (availableBits < width) {
        currentBytes |= ((long) bb.get() & 0xff) << availableBits;
        availableBits += 8;
      }
      longs[i] = (currentBytes & mask);
      currentBytes >>>= width;
      availableBits -= width;
    }
  }

  private static void writePackedInts64Over56bits(final MemoryOutputStream os, final long[] longs,
                                                  final int width, final int offset, final int length) {
    final long mask = width==64? (long)-1 : ~(((long)-1) << width);
    int shift = 0;
    long currentByteHi = 0;
    long currentByteLo = 0;
    for (int i=offset; i<offset+length; ++i) {
      currentByteLo |= (longs[i] & mask) << shift;
      currentByteHi = shift == 0? 0 : (longs[i] & mask) >>> (64 - shift);
      shift += width;
      while (shift >= 8) {
        os.write((int)currentByteLo);
        currentByteLo = (currentByteHi << 56) | (currentByteLo >>> 8);
        currentByteHi >>>= 8;
        shift -= 8;
      }
    }
    if (shift > 0) {
      os.write((int)currentByteLo);
    }
  }

  private static void readPackedInts64Over56bits(final ByteBuffer bb, final long[] longs, final int width,
                                                 final int offset, final int length) {
    final long mask = width==64? (long)-1 : ~(((long)(-1)) << width);
    long currentBytesLo = 0;
    long currentBytesHi = 0;
    int availableBits = 0;
    for (int i=0; i<length; ++i) {
      while (availableBits < width) {
        long b = (long) bb.get() & 0xff;
        currentBytesLo |= b << availableBits;
        currentBytesHi = b >>> (64 - availableBits);
        availableBits += 8;
      }
      longs[i] = (currentBytesLo & mask);
      currentBytesLo = width==64? 0 : (currentBytesHi << (64 - width)) | (currentBytesLo >>> width);
      availableBits -= width;
    }
  }

  public static void writeByteArray(final MemoryOutputStream os, final byte[] bytes) {
    if (bytes == null) {
      writeSInt(os, -1);
    } else {
      writeSInt(os, bytes.length);
      os.write(bytes);
    }
  }

  public static byte[] readByteArray(final ByteBuffer bb) {
    int length = readSInt(bb);
    if (length < 0){
      return null;
    }
    byte[] bytes = new byte[length];
    bb.get(bytes);
    return bytes;
  }

  public static void writeByteBuffer(final MemoryOutputStream os, final ByteBuffer bb) {
    if (bb == null) {
      writeSInt(os, -1);
    } else {
      writeSInt(os, bb.limit() - bb.position());
      os.write(bb);
    }
  }

  public static ByteBuffer readByteBuffer(final ByteBuffer bb) {
    int length = readSInt(bb);
    if (length < 0) {
      return null;
    }
    ByteBuffer byteBuffer = bb.slice();
    byteBuffer.limit(length);
    bb.position(bb.position() + length);
    return byteBuffer;
  }

  public static ByteBuffer sliceAhead(final ByteBuffer bb, final int numBytes) {
    ByteBuffer byteBuffer = bb.slice();
    byteBuffer.position(numBytes);
    return byteBuffer;
  }

}
