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
import java.io.OutputStream;

public class MemoryOutputStream extends OutputStream implements OutputBuffer {

  private static int DEFAULT_BUFFER_LENGTH = 1024;

  byte[] buffer;
  int position = 0;

  public MemoryOutputStream() {
    buffer = new byte[DEFAULT_BUFFER_LENGTH];
  }

  public MemoryOutputStream(final int length) {
    buffer = new byte[length];
  }

  @Override
  public void reset() {
    position = 0;
  }

  @Override
  public void finish() {}

  @Override
  public int length() {
    return position;
  }

  @Override
  public int estimatedLength() {
    return position;
  }

  @Override
  public void writeTo(final MemoryOutputStream mos) {
    mos.write(buffer, 0, position);
  }

  private void grow() {
    byte[] newBuffer = new byte[buffer.length << 1];
    System.arraycopy(buffer, 0, newBuffer, 0, buffer.length);
    buffer = newBuffer;
  }

  void ensureRemainingCapacity(final int capacity) {
    if (buffer.length - position < capacity) {
      grow();
      ensureRemainingCapacity(capacity);
    }
  }

  @Override
  public void write(final int b) {
    if (position == buffer.length) {
      grow();
      write(b);
    } else {
      buffer[position] = (byte)b;
      position += 1;
    }
  }

  @Override
  public void write(final byte[] bytes) {
    write(bytes, 0, bytes.length);
  }

  @Override
  public void write(final byte[] bytes, final int offset, final int length) {
    int bufferLength = buffer.length;
    while (position + length > bufferLength) {
      grow();
      bufferLength <<= 1;
    }
    System.arraycopy(bytes, offset, buffer, position, length);
    position += length;
  }

  public ByteBuffer byteBuffer() {
    return ByteBuffer.wrap(buffer, 0, position);
  }

}
