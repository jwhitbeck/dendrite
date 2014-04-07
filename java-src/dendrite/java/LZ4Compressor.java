package dendrite.java;

import java.util.zip.Deflater;

public class LZ4Compressor implements Compressor {

  private final ByteArrayWriter input_buffer;
  private final ByteArrayWriter output_buffer;

  public LZ4Compressor() {
    input_buffer = new ByteArrayWriter();
    output_buffer = new ByteArrayWriter();
  }

  @Override
  public void compress(final ByteArrayWritable byte_array_writable) {
    byte_array_writable.writeTo(input_buffer);
    net.jpountz.lz4.LZ4Compressor lz4_compressor = LZ4.compressor();
    output_buffer.ensureRemainingCapacity(lz4_compressor.maxCompressedLength(input_buffer.size()));
    int compressed_length = lz4_compressor.compress(input_buffer.buffer, 0, input_buffer.size(),
                                                output_buffer.buffer, 0);
    output_buffer.position += compressed_length;
  }

  @Override
  public void reset() {
    input_buffer.reset();
    output_buffer.reset();
  }

  @Override
  public int uncompressedSize() {
    return input_buffer.size();
  }

  @Override
  public int compressedSize() {
    return output_buffer.size();
  }

  @Override
  public void writeTo(final ByteArrayWriter baw) {
    output_buffer.writeTo(baw);
  }
}
