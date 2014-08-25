/**
* Copyright (c) 2013-2014 John Whitbeck. All rights reserved.
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

public class ByteArrayPlainDecoder extends AbstractDecoder {

  public ByteArrayPlainDecoder(final ByteArrayReader baw) {
    super(baw);
  }

  @Override
  public Object decode() {
    int length = byteArrayReader.readFixedInt();
    byte[] byteArray = new byte[length];
    byteArrayReader.readByteArray(byteArray, 0, length);
    return byteArray;
  }

}
