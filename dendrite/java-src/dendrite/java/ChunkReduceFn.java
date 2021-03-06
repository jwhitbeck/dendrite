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

import clojure.lang.AFn;

public final class ChunkReduceFn extends AFn {

  private final int initialChunkSize;

  public ChunkReduceFn(int initialChunkSize) {
    this.initialChunkSize = initialChunkSize;
  }

  @Override
  public Object invoke() {
    return new EditableChunk(initialChunkSize);
  }

  @Override
  public Object invoke(Object ret) {
    return ((EditableChunk)ret).finish();
  }

  @Override
  public Object invoke(Object ret, Object input) {
    return ((EditableChunk)ret).add(input);
  }
}
