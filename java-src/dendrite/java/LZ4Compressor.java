package dendrite.java;

import java.util.zip.Deflater;

public class LZ4Compressor implements Compressor {

  private int input_length = 0;
  private int compressed_length = 0;
  private byte[] input_buffer;
  private byte[] output_buffer;

  public LZ4Compressor() {
    output_buffer = new byte[1024];
  }

  private void ensureOutputBufferCapacity(final int capacity) {
    if (output_buffer.length < capacity) {
      output_buffer = new byte[(int)(capacity * 1.2)];
    }
  }

  @Override
  public void compress(final ByteArrayWriter baw) {
    input_length = baw.size();
    input_buffer = baw.buffer;
    net.jpountz.lz4.LZ4Compressor lz4_compressor = LZ4.compressor();
    int max_compressed_length = lz4_compressor.maxCompressedLength(input_length);
    ensureOutputBufferCapacity(max_compressed_length);
    compressed_length = lz4_compressor.compress(input_buffer, 0, input_length, output_buffer, 0);
  }

  @Override
  public void reset() {
    compressed_length = 0;
    input_length = 0;
  }

  @Override
  public int uncompressedSize() {
    return input_length;
  }

  @Override
  public int compressedSize() {
    return compressed_length;
  }

  @Override
  public void writeUncompressedTo(final ByteArrayWriter baw) {
    baw.writeByteArray(input_buffer, 0, input_length);
  }

  @Override
  public void writeCompressedTo(final ByteArrayWriter baw) {
    baw.writeByteArray(output_buffer, 0, compressed_length);
  }

}
