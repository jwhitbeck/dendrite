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

public class ByteArrayDeltaLengthDecoder extends AbstractDecoder {

  private final IntPackedDeltaDecoder lengthsDecoder;

  public ByteArrayDeltaLengthDecoder(final ByteArrayReader baw) {
    super(baw);
    int lengthsNumBytes = byteArrayReader.readUInt();
    lengthsDecoder = new IntPackedDeltaDecoder(byteArrayReader);
    byteArrayReader.position += lengthsNumBytes;
  }

  @Override
  public Object decode() {
    int length = lengthsDecoder.decodeInt();
    byte[] byteArray = new byte[length];
    byteArrayReader.readByteArray(byteArray, 0, length);
    return byteArray;
  }

  public void decodeInto(ByteArrayWriter baw) {
    int length = lengthsDecoder.decodeInt();
    byteArrayReader.readBytes(baw, length);
  }

}
