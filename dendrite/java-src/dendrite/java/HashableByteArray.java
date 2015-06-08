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

import java.util.Arrays;

public final class HashableByteArray {

  public final byte[] array;

  public HashableByteArray(byte[] array) {
    this.array = array;
  }

  @Override
  public int hashCode() {
    int hash = 1;
    for (int i=0; i<array.length; ++i) {
      hash = 31 * hash + (int)array[i];
    }
    return hash;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof HashableByteArray)) {
      return false;
    }
    return Arrays.equals(array, ((HashableByteArray)o).array);
  }
}
