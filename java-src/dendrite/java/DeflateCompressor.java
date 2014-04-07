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
  public void compress(final ByteArrayWritable byte_array_writable) {
    byte_array_writable.writeTo(input_buffer);
    deflater.setInput(input_buffer.buffer, 0, input_buffer.size());
    deflater.finish();
    output_buffer.ensureRemainingCapacity(input_buffer.size());
    deflater.deflate(output_buffer.buffer, 0, output_buffer.buffer.length - output_buffer.position);
    while (!deflater.finished()) {
      int prev_buffer_length = output_buffer.buffer.length;
      output_buffer.ensureRemainingCapacity(input_buffer.size());
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
