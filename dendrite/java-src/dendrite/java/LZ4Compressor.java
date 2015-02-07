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
