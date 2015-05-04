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

public final class Thresholds {

  public static int nextCheckThreshold(final int idx, final int value, final int target) {
    if (value == 0) {
      return idx + 1;
    }
    int nextIdx = idx + (idx * (target - value))/(2 * value);
    if (nextIdx == idx) {
      return idx + 1;
    }
    return nextIdx;
  }
}
