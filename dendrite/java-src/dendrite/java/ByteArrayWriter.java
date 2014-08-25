package dendrite.java;

import java.nio.ByteBuffer;
import java.math.BigInteger;

public class ByteArrayWriter implements Flushable, Resetable, Lengthable {

  private static int DEFAULT_BUFFER_LENGTH = 1024;

  public byte[] buffer;
  public int position = 0;

  public ByteArrayWriter() {
    buffer = new byte[DEFAULT_BUFFER_LENGTH];
  }

  public ByteArrayWriter(final int length) {
    buffer = new byte[length];
  }

  @Override
  public void reset() {
    position = 0;
  }

  private void grow() {
    byte[] newBuffer = new byte[buffer.length << 1];
    System.arraycopy(buffer, 0, newBuffer, 0, buffer.length);
    buffer = newBuffer;
  }

  public void ensureRemainingCapacity(final int capacity) {
    if (buffer.length - position < capacity) {
      grow();
      ensureRemainingCapacity(capacity);
    }
  }

  public int length() {
    return position;
  }

  public void writeByte(final byte b) {
    if (position == buffer.length) {
      grow();
      writeByte(b);
    } else {
      buffer[position] = b;
      position += 1;
    }
  }

  public void writeFixedInt(final int i){
    writeByte((byte)  i       );
    writeByte((byte) (i >>  8));
    writeByte((byte) (i >> 16));
    writeByte((byte) (i >> 24));
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

  public void writeUInt(final int i) {
    int value = i;
    while (true) {
      if ((value & ~0x7f) == 0) {
        writeByte((byte)value);
        return;
      } else {
        writeByte((byte) ((value & 0x7f) | 0x80));
        value >>>= 7;
      }
    }
  }

  private static int encodeZigZag32(final int i) {
    return (i << 1) ^ (i >> 31);
  }

  public void writeSInt(final int i) {
    writeUInt(encodeZigZag32(i));
  }

  public void writeFixedLong(final long l) {
    writeByte((byte)  l        );
    writeByte((byte) (l >>  8));
    writeByte((byte) (l >> 16));
    writeByte((byte) (l >> 24));
    writeByte((byte) (l >> 32));
    writeByte((byte) (l >> 40));
    writeByte((byte) (l >> 48));
    writeByte((byte) (l >> 56));
  }

  public void writeULong(final long l) {
    long value = l;
    while (true) {
      if ((value & ~0x7fL) == 0) {
        writeByte((byte)value);
        return;
      } else {
        writeByte((byte)((value & 0x7f) | 0x80));
        value >>>= 7;
      }
    }
  }

  private static long encodeZigZag64(final long l) {
    return (l << 1) ^ (l >> 63);
  }

  public void writeSLong(final long l) {
    writeULong(encodeZigZag64(l));
  }

  public void writeBigInt(final BigInteger bi) {
    byte[] intAsBytes = bi.toByteArray();
    writeUInt(intAsBytes.length);
    writeByteArray(intAsBytes);
  }

  public void writeUIntVLQ(final BigInteger bi) {
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
        writeByte((byte)((byteBuffer & 0x7f) | 0x80));
        byteBuffer >>>= 7;
        shift -= 7;
      }
    }
    writeByte((byte)byteBuffer);
  }

  public static BigInteger encodeZigZag(final BigInteger bi) {
    if ( bi.signum() < 0 ){
      return bi.negate().shiftLeft(1).add(BigInteger.ONE);
    } else {
      return bi.shiftLeft(1);
    }
  }

  public void writeSIntVLQ(final BigInteger bi) {
    writeUIntVLQ(encodeZigZag(bi));
  }

  public void writePackedBooleans(final boolean[] booleanOctuplet) {
    int b = 0;
    for (int i=0; i<8; ++i){
      b = (b << 1) | (booleanOctuplet[i]? 1 : 0);
    }
    writeByte((byte) b);
  }

  public void writeFloat(final float f) {
    writeFixedInt(Float.floatToRawIntBits(f));
  }

  public void writeDouble(final double d) {
    writeFixedLong(Double.doubleToRawLongBits(d));
  }

  public void writeByteArray(final byte[] bytes) {
    writeByteArray(bytes, 0, bytes.length);
  }

  public void writeByteArray(final byte[] bytes, final int length) {
    writeByteArray(bytes, 0, length);
  }

  public void writeByteArray(final byte[] bytes, final int offset, final int length) {
    int bufferLength = buffer.length;
    while (position + length > bufferLength) {
      grow();
      bufferLength <<= 1;
    }
    System.arraycopy(bytes, offset, buffer, position, length);
    position += length;
  }

  public static int getBitWidth(int i) {
    return 32 - Integer.numberOfLeadingZeros(i);
  }

  public static int getBitWidth(long l) {
    return 64 - Long.numberOfLeadingZeros(l);
  }

  public void writePackedInt(final int i, final int width) {
    final int mask = (width == 32)? -1 : ~((-1) << width);
    int currentByte = i & mask;
    int remainingWidth = width;
    while (remainingWidth > 0) {
      writeByte((byte) currentByte);
      currentByte >>>= 8;
      remainingWidth -= 8;
    }
  }

  public void writePackedInts32(final int[] ints, final int width, final int length) {
    writePackedInts32(ints, width, 0, length);
  }

  public void writePackedInts32(final int[] ints, final int width, final int offset, final int length) {
    if (width > 0) {
      if (width < 25) {
        writePackedInts32Under24Bits(ints, width, offset, length);
      } else {
        writePackedInts32Over24Bits(ints, width, offset, length);
      }
    }
  }

  private void writePackedInts32Under24Bits(final int[] ints, final int width,
                                            final int offset, final int length) {
    final int mask = ~((-1) << width);
    int shift = 0;
    int currentByte = 0;
    for (int i=offset; i<offset+length; ++i) {
      currentByte |= (ints[i] & mask) << shift;
      shift += width;
      while (shift >= 8) {
        writeByte((byte) currentByte);
        currentByte >>>= 8;
        shift -= 8;
      }
    }
    if (shift > 0) {
      writeByte((byte) currentByte);
    }
  }

  private void writePackedInts32Over24Bits(final int[] ints, final int width,
                                           final int offset, final int length) {
    final long mask = ~((long)-1 << width);
    int shift = 0;
    long currentByte = 0;
    for (int i=offset; i<offset+length; ++i) {
      currentByte |= ((long)ints[i] & mask) << shift;
      shift += width;
      while (shift >= 8) {
        writeByte((byte) currentByte);
        currentByte >>>= 8;
        shift -= 8;
      }
    }
    if (shift > 0) {
      writeByte((byte) currentByte);
    }
  }

  public void writePackedInts64(final long[] longs, final int width, final int length) {
    writePackedInts64(longs, width, 0, length);
  }

  public void writePackedInts64(final long[] longs, final int width, final int offset, final int length) {
    if (width > 0) {
      if (width < 57) {
        writePackedInts64Under56bits(longs, width, offset, length);
      } else {
        writePackedInts64Over56bits(longs, width, offset, length);
      }
    }
  }

  private void writePackedInts64Under56bits(final long[] longs, final int width,
                                            final int offset, final int length) {
    final long mask = ~(((long)-1) << width);
    int shift = 0;
    long currentByte = 0;
    for (int i=offset; i<offset+length; ++i) {
      currentByte |= (longs[i] & mask) << shift;
      shift += width;
      while (shift >= 8) {
        writeByte((byte) currentByte);
        currentByte >>>= 8;
        shift -= 8;
      }
    }
    if (shift > 0) {
      writeByte((byte) currentByte);
    }
  }

  private void writePackedInts64Over56bits(final long[] longs, final int width,
                                           final int offset, final int length) {
    final long mask = width==64? (long)-1 : ~(((long)-1) << width);
    int shift = 0;
    long currentByteHi = 0;
    long currentByteLo = 0;
    for (int i=offset; i<offset+length; ++i) {
      currentByteLo |= (longs[i] & mask) << shift;
      currentByteHi = shift == 0? 0 : (longs[i] & mask) >>> (64 - shift);
      shift += width;
      while (shift >= 8) {
        writeByte((byte) currentByteLo);
        currentByteLo = (currentByteHi << 56) | (currentByteLo >>> 8);
        currentByteHi >>>= 8;
        shift -= 8;
      }
    }
    if (shift > 0) {
      writeByte((byte) currentByteLo);
    }
  }

  @Override
  public void flush(final ByteArrayWriter baw) {
    baw.writeByteArray(buffer, position);
  }

  public void flush(final ByteBuffer bb) {
    bb.put(buffer, 0, position);
  }

  public void write(final Flushable flushable) {
    flushable.flush(this);
  }

  public void write(final ByteBuffer byteBuffer) {
    int bufferLength = buffer.length;
    int newDataLength = byteBuffer.limit() - byteBuffer.position();
    while (position + newDataLength > bufferLength) {
      grow();
      bufferLength <<= 1;
    }
    byteBuffer.get(buffer, position, newDataLength);
    position += newDataLength;
  }

}
