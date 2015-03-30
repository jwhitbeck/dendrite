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

public class LZ4Compressor implements ICompressor {

  private final MemoryOutputStream inputBuffer;
  private final MemoryOutputStream outputBuffer;

  public LZ4Compressor() {
    inputBuffer = new MemoryOutputStream();
    outputBuffer = new MemoryOutputStream();
  }

  @Override
  public void compress(final IOutputBuffer buffer) {
    buffer.writeTo(inputBuffer);
    net.jpountz.lz4.LZ4Compressor lz4Compressor = LZ4.compressor();
    outputBuffer.ensureRemainingCapacity(lz4Compressor.maxCompressedLength(inputBuffer.length()));
    int compressedLength = lz4Compressor.compress(inputBuffer.buffer, 0, inputBuffer.length(),
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
  public void writeTo(final MemoryOutputStream mos) {
    outputBuffer.writeTo(mos);
  }
}
