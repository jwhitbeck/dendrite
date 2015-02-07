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

public abstract class AbstractEncoder implements Encoder {

  protected final ByteArrayWriter byteArrayWriter;
  protected int numValues = 0;

  public AbstractEncoder() {
    this.byteArrayWriter = new ByteArrayWriter();
  }

  @Override
  public void reset() {
    numValues = 0;
    byteArrayWriter.reset();
  }

  @Override
  public void finish() {}

  @Override
  public int length() {
    return ByteArrayWriter.getNumUIntBytes(numValues) +  byteArrayWriter.length();
  }

  @Override
  public int estimatedLength() {
    return length();
  }

  @Override
  public void flush(final ByteArrayWriter baw) {
    finish();
    baw.writeUInt(numValues);
    byteArrayWriter.flush(baw);
  }

  @Override
  public int numEncodedValues() {
    return numValues;
  }

}
