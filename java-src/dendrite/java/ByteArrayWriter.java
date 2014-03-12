package dendrite.java;

public class ByteArrayWriter implements Resetable {

  private static int DEFAULT_BUFFER_SIZE = 1024;

  public byte[] buffer;
  private int position = 0;

  public ByteArrayWriter() {
    buffer = new byte[DEFAULT_BUFFER_SIZE];
  }

  public ByteArrayWriter(final int size) {
    buffer = new byte[size];
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

  public int size() {
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

  public void writeFixedInt32(final int i){
    writeByte((byte)  (i        & 0xff));
    writeByte((byte) ((i >>  8) & 0xff));
    writeByte((byte) ((i >> 16) & 0xff));
    writeByte((byte) ((i >> 24) & 0xff));
  }

  public void writeUInt32(final int i) {
    int value = i;
    while (true) {
      if ((value & ~0x7F) == 0) {
        writeByte((byte)value);
        return;
      } else {
        writeByte((byte) ((value & 0x7F) | 0x80));
        value >>>= 7;
      }
    }
  }

  private static int encodeZigZag32(final int i) {
    return (i << 1) ^ (i >> 31);
  }

  public void writeSInt32(final int i) {
    writeUInt32(encodeZigZag32(i));
  }

  public void writeFixedInt64(final long l) {
    writeByte((byte)  (l        & 0xffL));
    writeByte((byte) ((l >>  8) & 0xffL));
    writeByte((byte) ((l >> 16) & 0xffL));
    writeByte((byte) ((l >> 24) & 0xffL));
    writeByte((byte) ((l >> 32) & 0xffL));
    writeByte((byte) ((l >> 40) & 0xffL));
    writeByte((byte) ((l >> 48) & 0xffL));
    writeByte((byte) ((l >> 56) & 0xffL));
  }

  public void writeUInt64(final long l) {
    long value = l;
    while (true) {
      if ((value & ~0x7FL) == 0) {
        writeByte((byte)value);
        return;
      } else {
        writeByte((byte)((value & 0x7F) | 0x80));
        value >>>= 7;
      }
    }
  }

  private static long encodeZigZag64(final long l) {
    return (l << 1) ^ (l >> 63);
  }

  public void writeSInt64(final long l) {
    writeUInt64(encodeZigZag64(l));
  }

  public void writePackedBooleans(final boolean[] booleanOctuplet) {
    int b = 0;
    for (int i=0; i<8; ++i){
      b = (b << 1) | (booleanOctuplet[i]? 1 : 0);
    }
    writeByte((byte)(b & 0xff));
  }

  public void writeFloat(final float f) {
    writeFixedInt32(Float.floatToRawIntBits(f));
  }

  public void writeDouble(final double d) {
    writeFixedInt64(Double.doubleToRawLongBits(d));
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

  public void writePackedInts32(final int[] ints, final int width, final int length) {
    final int mask = ~((-1) << width);
    int shift = 0;
    int current_byte = 0;
    for (int i=0; i<length; ++i) {
      current_byte |= (ints[i] & mask) << shift;
      shift += width;
      while (shift >= 8) {
        writeByte((byte)(current_byte & 0xff));
        current_byte >>>= 8;
        shift -= 8;
      }
    }
    if (shift > 0) {
      writeByte((byte)(current_byte & 0xff));
    }
  }

  public void writePackedInts64(final long[] longs, final int width, final int length) {
    final long mask = ~(((long)-1) << width);
    int shift = 0;
    long current_byte = 0;
    for (int i=0; i<length; ++i) {
      current_byte |= (longs[i] & mask) << shift;
      shift += width;
      while (shift >= 8) {
        writeByte((byte)(current_byte & 0xff));
        current_byte >>>= 8;
        shift -= 8;
      }
    }
    if (shift > 0) {
      writeByte((byte)(current_byte & 0xff));
    }
  }

  public void copy(final ByteArrayWriter baw) {
    baw.writeByteArray(buffer, position);
  }

}
