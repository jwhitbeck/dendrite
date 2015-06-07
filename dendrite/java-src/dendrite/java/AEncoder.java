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

public abstract class AEncoder implements IEncoder {

  protected final MemoryOutputStream mos;
  protected int numValues = 0;

  public AEncoder() {
    this.mos = new MemoryOutputStream();
  }

  @Override
  public void reset() {
    numValues = 0;
    mos.reset();
  }

  @Override
  public void finish() {}

  @Override
  public int getLength() {
    return Bytes.getNumUIntBytes(numValues) +  mos.getLength();
  }

  @Override
  public int getEstimatedLength() {
    return getLength();
  }

  @Override
  public void writeTo(final MemoryOutputStream memoryOutputStream) {
    finish();
    Bytes.writeUInt(memoryOutputStream, numValues);
    mos.writeTo(memoryOutputStream);
  }

  @Override
  public int getNumEncodedValues() {
    return numValues;
  }

}
