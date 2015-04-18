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

public class ByteArrayIncrementalDecoder extends ADecoder {

  private final ByteArrayDeltaLengthDecoder byteArrayDecoder;
  private final IntPackedDeltaDecoder prefixLengthsDecoder;
  private final MemoryOutputStream buffer;

  public ByteArrayIncrementalDecoder(final ByteBuffer byteBuffer) {
    super(byteBuffer);
    int prefixLengthsNumBytes = Bytes.readUInt(bb);
    prefixLengthsDecoder = new IntPackedDeltaDecoder(bb);
    ByteBuffer byteArrayDeltaLengths = bb.slice();
    byteArrayDeltaLengths.position(byteArrayDeltaLengths.position() + prefixLengthsNumBytes);
    byteArrayDecoder = new ByteArrayDeltaLengthDecoder(byteArrayDeltaLengths);
    buffer = new MemoryOutputStream(128);
  }

  @Override
  public Object decode() {
    int prefixLength = prefixLengthsDecoder.decodeInt();
    buffer.position = prefixLength;
    byteArrayDecoder.decodeInto(buffer);
    byte[] byteArray = new byte[buffer.length()];
    System.arraycopy(buffer.buffer, 0, byteArray, 0, buffer.length());
    return byteArray;
  }

}
