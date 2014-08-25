package dendrite.java;

import java.util.zip.Deflater;

public class DeflateCompressor implements Compressor {

  private final ByteArrayWriter inputBuffer;
  private final ByteArrayWriter outputBuffer;
  private final Deflater deflater;

  public DeflateCompressor() {
    deflater = new Deflater(Deflater.DEFAULT_COMPRESSION, true);
    inputBuffer = new ByteArrayWriter();
    outputBuffer = new ByteArrayWriter();
  }

  @Override
  public void compress(final Flushable flushable) {
    flushable.flush(inputBuffer);
    deflater.setInput(inputBuffer.buffer, 0, inputBuffer.length());
    deflater.finish();
    outputBuffer.ensureRemainingCapacity(inputBuffer.length());
    deflater.deflate(outputBuffer.buffer, 0, outputBuffer.buffer.length - outputBuffer.position);
    while (!deflater.finished()) {
      int prevBufferLength = outputBuffer.buffer.length;
      outputBuffer.ensureRemainingCapacity(inputBuffer.length());
      deflater.deflate(outputBuffer.buffer, prevBufferLength,
                       outputBuffer.buffer.length - prevBufferLength);
    }
    outputBuffer.position += (int)deflater.getBytesWritten();
  }

  @Override
  public void reset() {
    deflater.reset();
    inputBuffer.reset();
    outputBuffer.reset();
  }

  @Override
  public int uncompressedLength() {
    return inputBuffer.length();
  }

  @Override
  public int compressedLength() {
    return outputBuffer.length();
  }

  @Override
  public void flush(final ByteArrayWriter baw) {
    outputBuffer.flush(baw);
  }


}
