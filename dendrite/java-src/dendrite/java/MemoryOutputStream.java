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

import java.io.OutputStream;
import java.nio.ByteBuffer;

public class MemoryOutputStream extends OutputStream implements IOutputBuffer {

  private static int DEFAULT_BUFFER_LENGTH = 1024;

  byte[] buffer;
  int position = 0;

  public MemoryOutputStream() {
    buffer = new byte[DEFAULT_BUFFER_LENGTH];
  }

  public MemoryOutputStream(int length) {
    buffer = new byte[length];
  }

  @Override
  public void reset() {
    position = 0;
  }

  @Override
  public void finish() {}

  @Override
  public int getLength() {
    return position;
  }

  @Override
  public int getEstimatedLength() {
    return position;
  }

  @Override
  public void writeTo(MemoryOutputStream mos) {
    mos.write(buffer, 0, position);
  }

  private void grow() {
    byte[] newBuffer = new byte[buffer.length << 1];
    System.arraycopy(buffer, 0, newBuffer, 0, buffer.length);
    buffer = newBuffer;
  }

  void ensureRemainingCapacity(int capacity) {
    while (buffer.length - position < capacity) {
      grow();
    }
  }

  @Override
  public void write(int b) {
    if (position == buffer.length) {
      grow();
      write(b);
    } else {
      buffer[position] = (byte)b;
      position += 1;
    }
  }

  @Override
  public void write(byte[] bytes) {
    write(bytes, 0, bytes.length);
  }

  @Override
  public void write(byte[] bytes, int offset, int length) {
    ensureRemainingCapacity(length);
    System.arraycopy(bytes, offset, buffer, position, length);
    position += length;
  }

  public void write(ByteBuffer bb) {
    bb.mark();
    int length = bb.remaining();
    ensureRemainingCapacity(length);
    bb.get(buffer, position, length);
    position += length;
    bb.reset();
  }

  public void write(IWriteable writable) {
    writable.writeTo(this);
  }

  public ByteBuffer toByteBuffer() {
    return ByteBuffer.wrap(buffer, 0, position);
  }

}
