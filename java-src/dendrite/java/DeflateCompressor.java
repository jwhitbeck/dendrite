package dendrite.java;

import java.util.zip.Deflater;

public class DeflateCompressor implements Compressor {

  private int input_length = 0;
  private int compressed_length = 0;
  private final Deflater deflater;

  public DeflateCompressor() {
    deflater = new Deflater(Deflater.DEFAULT_COMPRESSION, true);
  }

  @Override
  public void compress(final byte[] bs, final int offset, final int length, final ByteArrayWriter baw) {
    input_length = length;
    deflater.setInput(bs, offset, input_length);
    deflater.finish();
    baw.ensureRemainingCapacity(input_length);
    deflater.deflate(baw.buffer, baw.size(), baw.buffer.length - baw.size());
    while (!deflater.finished()) {
      int prev_buffer_length = baw.buffer.length;
      baw.ensureRemainingCapacity(input_length);
      deflater.deflate(baw.buffer, prev_buffer_length, baw.buffer.length - prev_buffer_length);
    }
    compressed_length = (int)deflater.getBytesWritten();
    baw.skipAhead(compressedSize());
  }

  @Override
  public void reset() {
    deflater.reset();
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
