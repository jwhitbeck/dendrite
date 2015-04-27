/**
 * Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
 *
 * The use and distribution terms for this software are covered by the
 * Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
 * which can be found in the file epl-v10.txt at the root of this distribution.
 * By using this software in any fashion, you are agreeing to be bound by
 * the terms of this license.
 *
 * You must not remove this notice, or any other, from this software.
 */

package dendrite.java;

import java.nio.ByteBuffer;
import java.util.zip.Deflater;
import java.util.zip.Inflater;
import java.util.zip.DataFormatException;


public final class Deflate {

  public final static class Compressor implements ICompressor {

    private final MemoryOutputStream inputBuffer;
    private final MemoryOutputStream outputBuffer;
    private final Deflater deflater;

    public Compressor() {
      deflater = new Deflater(Deflater.DEFAULT_COMPRESSION, true);
      inputBuffer = new MemoryOutputStream();
      outputBuffer = new MemoryOutputStream();
    }

    @Override
    public void compress(IOutputBuffer buffer) {
      buffer.writeTo(inputBuffer);
      deflater.setInput(inputBuffer.buffer, 0, inputBuffer.length());
      deflater.finish();
      outputBuffer.ensureRemainingCapacity(inputBuffer.length());
      deflater.deflate(outputBuffer.buffer, 0, outputBuffer.buffer.length - outputBuffer.position);
      while (!deflater.finished()) {
        int prevBufferLength = outputBuffer.buffer.length;
        outputBuffer.ensureRemainingCapacity(prevBufferLength + inputBuffer.length());
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
    public int length() {
      return outputBuffer.length();
    }

    @Override
    public int estimatedLength() {
      return outputBuffer.length();
    }

    @Override
    public void finish() {}

    @Override
    public void writeTo(MemoryOutputStream mos) {
      outputBuffer.writeTo(mos);
    }

  }

  public final static class Decompressor implements IDecompressor {

    @Override
    public ByteBuffer decompress(ByteBuffer byteBuffer, int compressedLength, int decompressedLength) {
      Inflater inflater = new Inflater(true);
      byte[] compressedBytes;
      int offset;
      if (byteBuffer.hasArray()) {
        compressedBytes = byteBuffer.array();
        offset = byteBuffer.arrayOffset() + byteBuffer.position();
        byteBuffer.position(byteBuffer.position() + compressedLength);
      } else {
        compressedBytes = new byte[compressedLength];
        byteBuffer.get(compressedBytes);
        offset = 0;
      }
      inflater.setInput(compressedBytes, offset, compressedLength);
      byte[] decompressedBytes = new byte[decompressedLength];
      try {
        inflater.inflate(decompressedBytes);
      } catch (DataFormatException dfe) {
        throw new IllegalStateException(dfe);
      } finally {
        inflater.end();
      }
      return ByteBuffer.wrap(decompressedBytes);
    }

  }

  public final static IDecompressorFactory decompressorFactory = new IDecompressorFactory() {
      public IDecompressor create() {
        return new Decompressor();
      }
    };

}
