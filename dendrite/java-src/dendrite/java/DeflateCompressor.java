package dendrite.java;

import java.util.zip.Deflater;

public class DeflateCompressor implements Compressor {

  private final ByteArrayWriter input_buffer;
  private final ByteArrayWriter output_buffer;
  private final Deflater deflater;

  public DeflateCompressor() {
    deflater = new Deflater(Deflater.DEFAULT_COMPRESSION, true);
    input_buffer = new ByteArrayWriter();
    output_buffer = new ByteArrayWriter();
  }

  @Override
  public void compress(final Flushable flushable) {
    flushable.flush(input_buffer);
    deflater.setInput(input_buffer.buffer, 0, input_buffer.length());
    deflater.finish();
    output_buffer.ensureRemainingCapacity(input_buffer.length());
    deflater.deflate(output_buffer.buffer, 0, output_buffer.buffer.length - output_buffer.position);
    while (!deflater.finished()) {
      int prev_buffer_length = output_buffer.buffer.length;
      output_buffer.ensureRemainingCapacity(input_buffer.length());
      deflater.deflate(output_buffer.buffer, prev_buffer_length,
                       output_buffer.buffer.length - prev_buffer_length);
    }
    output_buffer.position += (int)deflater.getBytesWritten();
  }

  @Override
  public void reset() {
    deflater.reset();
    input_buffer.reset();
    output_buffer.reset();
  }

  @Override
  public int uncompressedLength() {
    return input_buffer.length();
  }

  @Override
  public int compressedLength() {
    return output_buffer.length();
  }

  @Override
  public void flush(final ByteArrayWriter baw) {
    output_buffer.flush(baw);
  }


}
