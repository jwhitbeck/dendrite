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

public class ByteArrayDeltaLengthDecoder extends ADecoder {

  private final IntPackedDeltaDecoder lengthsDecoder;

  public ByteArrayDeltaLengthDecoder(final ByteBuffer byteBuffer) {
    super(byteBuffer);
    int lengthsNumBytes = Bytes.readUInt(bb);
    lengthsDecoder = new IntPackedDeltaDecoder(bb);
    bb.position(bb.position() + lengthsNumBytes);
  }

  @Override
  public Object decode() {
    int length = lengthsDecoder.decodeInt();
    byte[] byteArray = new byte[length];
    bb.get(byteArray, 0, length);
    return byteArray;
  }

  public void decodeInto(final MemoryOutputStream mos) {
    int length = lengthsDecoder.decodeInt();
    mos.ensureRemainingCapacity(length);
    bb.get(mos.buffer, mos.position, length);
    mos.position += length;
  }

}
