package dendrite.java;

import java.util.zip.Deflater;

public class DeflateCompressor implements Compressor {

  private int input_length = 0;
  private byte[] input_buffer;
  private final Deflater deflater;
  private byte[] output_buffer;
  private int position = 0;

  public DeflateCompressor() {
    deflater = new Deflater(Deflater.DEFAULT_COMPRESSION, true);
    output_buffer = new byte[1024];
  }

  private void growOutputBuffer() {
    byte[] new_output_buffer = new byte[output_buffer.length << 1];
    System.arraycopy(output_buffer, 0, new_output_buffer, 0, output_buffer.length);
    position = output_buffer.length;
  }

  @Override
  public void compressBytes(final ByteArrayWriter baw) {
    input_length = baw.size();
    input_buffer = baw.buffer;
    deflater.setInput(input_buffer, 0, input_length);
    deflater.finish();
    deflater.deflate(output_buffer, position, output_buffer.length - position);
    while ( ! deflater.finished() ){
      growOutputBuffer();
      deflater.deflate(output_buffer, position, output_buffer.length - position);
    }
  }

  @Override
  public void reset() {
    deflater.reset();
    position = 0;
    input_length = 0;
  }

  @Override
  public int uncompressedSize() {
    return input_length;
  }

  @Override
  public int compressedSize() {
    return (int)deflater.getBytesWritten();
  }

  @Override
  public void writeUncompressedTo(final ByteArrayWriter baw) {
    baw.writeByteArray(input_buffer, 0, input_length);
  }

  @Override
  public void writeCompressedTo(final ByteArrayWriter baw) {
    baw.writeByteArray(output_buffer, 0, compressedSize());
  }

}
