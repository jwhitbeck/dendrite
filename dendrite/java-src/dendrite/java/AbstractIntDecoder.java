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

public abstract class AbstractIntDecoder extends AbstractDecoder implements IntDecoder {

  public AbstractIntDecoder(final ByteArrayReader byteArrayReader) {
    super(byteArrayReader);
  }

  @Override
  public Object decode() {
    return decodeInt();
  }

  @Override
  public IntIterator intIterator() {
    return new IntIterator() {
      int i = 0;
      @Override
      public boolean hasNext() {
        return i < numValues;
      }
      @Override
      public int next() {
        int v = decodeInt();
        i += 1;
        return v;
      }
    };
  }
}
