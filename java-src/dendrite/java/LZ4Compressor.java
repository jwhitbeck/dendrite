package dendrite.java;

import java.util.zip.Deflater;

public class LZ4Compressor implements Compressor {

  private int input_length = 0;
  private int compressed_length = 0;

  @Override
  public void compress(final byte[] bs, final int offset, final int length, final ByteArrayWriter baw) {
    input_length = length;
    net.jpountz.lz4.LZ4Compressor lz4_compressor = LZ4.compressor();
    baw.ensureRemainingCapacity(lz4_compressor.maxCompressedLength(input_length));
    compressed_length = lz4_compressor.compress(bs, offset, input_length, baw.buffer, baw.size());
    baw.skipAhead(compressed_length);
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
}
