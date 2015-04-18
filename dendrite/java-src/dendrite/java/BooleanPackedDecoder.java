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

public class BooleanPackedDecoder extends ADecoder {

  private final boolean[] octuplet = new boolean[8];
  private int position = 0;

  public BooleanPackedDecoder(final ByteBuffer byteBuffer){
    super(byteBuffer);
  }

  @Override
  public Object decode() {
    if ((position % 8) == 0) {
      Bytes.readPackedBooleans(bb, octuplet);
      position = 0;
    }
    boolean b = octuplet[position];
    position += 1;
    return b;
  }

}
