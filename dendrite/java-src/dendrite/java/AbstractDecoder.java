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

public abstract class AbstractDecoder implements Decoder {

  protected final ByteArrayReader byteArrayReader;
  protected final int numValues;

  public AbstractDecoder(final ByteArrayReader byteArrayReader) {
    this.byteArrayReader = byteArrayReader.slice();
    this.numValues = this.byteArrayReader.readUInt();
  }

  @Override
  public int numEncodedValues() {
    return numValues;
  }
}
