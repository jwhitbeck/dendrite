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

public class FixedLengthByteArrayPlainDecoder extends ADecoder {

  private final int length;

  public FixedLengthByteArrayPlainDecoder(final ByteBuffer byteBuffer) {
    super(byteBuffer);
    this.length = Bytes.readUInt(bb);
  }

  @Override
  public Object decode() {
    byte[] fixedLengthByteArray = new byte[length];
    bb.get(fixedLengthByteArray, 0, length);
    return fixedLengthByteArray;
  }

}
