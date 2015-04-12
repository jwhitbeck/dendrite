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

public final class Estimator {

  private long totalEstimated = 0;
  private long totalObserved = 0;

  public long correct(final long estimate) {
    if (totalEstimated == 0) {
      return estimate;
    }
    return (estimate * totalObserved) / totalEstimated;
  }

  public void update(final long observed, final long estimated) {
    totalEstimated += estimated;
    totalObserved += observed;
  }

  public static long nextCheckThreshold(final long idx, final long value, final long target) {
    if (value == 0) {
      return idx + 1;
    }
    long nextIdx = idx + (idx * (target - value))/(2 * value);
    if (nextIdx == idx) {
      return idx + 1;
    }
    return nextIdx;
  }
}
