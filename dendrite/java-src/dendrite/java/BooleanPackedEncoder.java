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

public class BooleanPackedEncoder extends AbstractEncoder {

  private boolean[] octuplet = new boolean[8];
  private int position = 0;

  @Override
  public void encode(final Object o) {
    final boolean b = (boolean) o;
    numValues += 1;
    octuplet[position] = b;
    position += 1;
    if (position == 8) {
      byteArrayWriter.writePackedBooleans(octuplet);
      position = 0;
    }
  }

  @Override
  public void reset() {
    position = 0;
    super.reset();
  }

  @Override
  public void finish() {
    if (position > 0){
      byteArrayWriter.writePackedBooleans(octuplet);
      position = 0;
    }
  }

  @Override
  public int estimatedLength() {
    return super.estimatedLength() + (position == 0? 0 : 1);
  }
}
