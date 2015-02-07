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

import java.util.Iterator;

public class IntPackedRunLengthDecoder implements IntDecoder {

  private final IntFixedBitWidthPackedRunLengthDecoder int32Decoder;

  public IntPackedRunLengthDecoder(final ByteArrayReader baw) {
    ByteArrayReader byteArrayReader = baw.slice();
    int width = (int)byteArrayReader.readByte() & 0xff;
    int32Decoder = new IntFixedBitWidthPackedRunLengthDecoder(byteArrayReader, width);
  }

  @Override
  public Object decode() {
    return int32Decoder.decode();
  }

  @Override
  public int decodeInt() {
    return int32Decoder.decodeInt();
  }

  @Override
  public int numEncodedValues() {
    return int32Decoder.numEncodedValues();
  }
}
