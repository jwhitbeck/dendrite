package dendrite.java;

import java.util.zip.Deflater;

public class LZ4Compressor implements Compressor {

  private final ByteArrayWriter inputBuffer;
  private final ByteArrayWriter outputBuffer;

  public LZ4Compressor() {
    inputBuffer = new ByteArrayWriter();
    outputBuffer = new ByteArrayWriter();
  }

  @Override
  public void compress(final Flushable flushable) {
    flushable.flush(inputBuffer);
    net.jpountz.lz4.LZ4Compressor lz4_compressor = LZ4.compressor();
    outputBuffer.ensureRemainingCapacity(lz4_compressor.maxCompressedLength(inputBuffer.length()));
    int compressedLength = lz4_compressor.compress(inputBuffer.buffer, 0, inputBuffer.length(),
                                                   outputBuffer.buffer, 0);
    outputBuffer.position += compressedLength;
  }

  @Override
  public void reset() {
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
