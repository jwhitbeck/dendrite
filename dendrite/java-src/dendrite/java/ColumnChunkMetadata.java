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

import clojure.lang.Keyword;

import java.nio.ByteBuffer;

public final class ColumnChunkMetadata implements IWriteable {

  public final int length;
  public final int numDataPages;
  public final int dataPageOffset;
  public final int dictionaryPageOffset;

  public ColumnChunkMetadata(int length, int numDataPages, int dataPageOffset, int dictionaryPageOffset) {
    this.length = length;
    this.numDataPages = numDataPages;
    this.dataPageOffset = dataPageOffset;
    this.dictionaryPageOffset = dictionaryPageOffset;
  }

  @Override
  public void writeTo(MemoryOutputStream mos) {
    Bytes.writeUInt(mos, length);
    Bytes.writeUInt(mos, numDataPages);
    Bytes.writeUInt(mos, dataPageOffset);
    Bytes.writeUInt(mos, dictionaryPageOffset);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    ColumnChunkMetadata ccm = (ColumnChunkMetadata) o;
    return length == ccm.length &&
      numDataPages == ccm.numDataPages &&
      dataPageOffset == ccm.dataPageOffset &&
      dictionaryPageOffset == ccm.dictionaryPageOffset;
  }

  public static ColumnChunkMetadata read(ByteBuffer bb) {
    return new ColumnChunkMetadata(Bytes.readUInt(bb),
                                   Bytes.readUInt(bb),
                                   Bytes.readUInt(bb),
                                   Bytes.readUInt(bb));
  }

}
