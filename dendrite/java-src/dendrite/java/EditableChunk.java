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

import clojure.lang.ArrayChunk;
import clojure.lang.IChunk;

public final class EditableChunk {

  private Object[] buffer;
  private int n;

  public EditableChunk(int initialSize) {
    buffer = new Object[initialSize];
    n = 0;
  }

  private void grow() {
    Object[] newBuffer = new Object[buffer.length << 1];
    System.arraycopy(buffer, 0, newBuffer, 0, buffer.length);
    buffer = newBuffer;
  }

  public EditableChunk add(Object o) {
    if (n == buffer.length) {
      grow();
    }
    buffer[n] = o;
    n += 1;
    return this;
  }

  public IChunk finish() {
    return new ArrayChunk(buffer, 0, n);
  }

}
