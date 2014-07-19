package dendrite.java;

import java.nio.ByteBuffer;
import java.math.BigInteger;

public class ByteArrayWriter implements ByteArrayWritable, Resetable, Lengthable {

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
    byte[] new_buffer = new byte[buffer.length << 1];
    System.arraycopy(buffer, 0, new_buffer, 0, buffer.length);
    buffer = new_buffer;
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
    int num_bytes = 0;
    int v = i;
    do {
      num_bytes += 1;
      v >>>= 7;
    } while (v != 0);
    return num_bytes;
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
    byte[] int_as_bytes = bi.toByteArray();
    writeUInt(int_as_bytes.length);
    writeByteArray(int_as_bytes);
  }

  public void writeUIntVLQ(final BigInteger bi) {
    byte[] int_as_bytes = bi.toByteArray();
    int num_bytes = int_as_bytes.length;
    int byte_buffer = 0;
    int shift = 0;
    for (int i=num_bytes-1; i>=0; --i) {
      int value = int_as_bytes[i] & 0xff;
      int bit_width = i==0? getBitWidth(value) : 8;
      byte_buffer |= value << shift;
      shift += bit_width;
      while (shift >= 7) {
        writeByte((byte)((byte_buffer & 0x7f) | 0x80));
        byte_buffer >>>= 7;
        shift -= 7;
      }
    }
    writeByte((byte)byte_buffer);
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
    int buffer_length = buffer.length;
    while (position + length > buffer_length) {
      grow();
      buffer_length <<= 1;
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
    int current_byte = i & mask;
    int remaining_width = width;
    while (remaining_width > 0) {
      writeByte((byte) current_byte);
      current_byte >>>= 8;
      remaining_width -= 8;
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
    int current_byte = 0;
    for (int i=offset; i<offset+length; ++i) {
      current_byte |= (ints[i] & mask) << shift;
      shift += width;
      while (shift >= 8) {
        writeByte((byte) current_byte);
        current_byte >>>= 8;
        shift -= 8;
      }
    }
    if (shift > 0) {
      writeByte((byte) current_byte);
    }
  }

  private void writePackedInts32Over24Bits(final int[] ints, final int width,
                                           final int offset, final int length) {
    final long mask = ~((long)-1 << width);
    int shift = 0;
    long current_byte = 0;
    for (int i=offset; i<offset+length; ++i) {
      current_byte |= ((long)ints[i] & mask) << shift;
      shift += width;
      while (shift >= 8) {
        writeByte((byte) current_byte);
        current_byte >>>= 8;
        shift -= 8;
      }
    }
    if (shift > 0) {
      writeByte((byte) current_byte);
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
    long current_byte = 0;
    for (int i=offset; i<offset+length; ++i) {
      current_byte |= (longs[i] & mask) << shift;
      shift += width;
      while (shift >= 8) {
        writeByte((byte) current_byte);
        current_byte >>>= 8;
        shift -= 8;
      }
    }
    if (shift > 0) {
      writeByte((byte) current_byte);
    }
  }

  private void writePackedInts64Over56bits(final long[] longs, final int width,
                                           final int offset, final int length) {
    final long mask = width==64? (long)-1 : ~(((long)-1) << width);
    int shift = 0;
    long current_byte_hi = 0;
    long current_byte_lo = 0;
    for (int i=offset; i<offset+length; ++i) {
      current_byte_lo |= (longs[i] & mask) << shift;
      current_byte_hi = shift == 0? 0 : (longs[i] & mask) >>> (64 - shift);
      shift += width;
      while (shift >= 8) {
        writeByte((byte) current_byte_lo);
        current_byte_lo = (current_byte_hi << 56) | (current_byte_lo >>> 8);
        current_byte_hi >>>= 8;
        shift -= 8;
      }
    }
    if (shift > 0) {
      writeByte((byte) current_byte_lo);
    }
  }

  @Override
  public void writeTo(final ByteArrayWriter baw) {
    baw.writeByteArray(buffer, position);
  }

  public void writeTo(final ByteBuffer bb) {
    bb.put(buffer, 0, position);
  }

  public void write(final ByteArrayWritable writable) {
    writable.writeTo(this);
  }

  public void write(final ByteBuffer byte_buffer) {
    int buffer_length = buffer.length;
    int new_data_length = byte_buffer.limit() - byte_buffer.position();
    while (position + new_data_length > buffer_length) {
      grow();
      buffer_length <<= 1;
    }
    byte_buffer.get(buffer, position, new_data_length);
    position += new_data_length;
  }

}
