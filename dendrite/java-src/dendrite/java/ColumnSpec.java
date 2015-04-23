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

public final class ColumnSpec implements IWriteable {

  public final int type;
  public final int encoding;
  public final int compression;
  public final int columnIndex;
  public final int maxRepetitionLevel;
  public final int maxDefinitionLevel;

  private final static int
    NULL = 0,
    PRESENT = 1;

  public ColumnSpec(int type, int encoding, int compression, int columnIndex, int maxRepetitionLevel,
                    int maxDefinitionLevel) {
    this.type = type;
    this.encoding = encoding;
    this.compression = compression;
    this.columnIndex = columnIndex;
    this.maxRepetitionLevel = maxRepetitionLevel;
    this.maxDefinitionLevel = maxDefinitionLevel;
  }

  @Override
  public void writeTo(MemoryOutputStream mos) {
    mos.write(PRESENT); // ColumSpec can be null. If so, the first byte will be 0;
    Bytes.writeSInt(mos, type);
    Bytes.writeUInt(mos, encoding);
    Bytes.writeUInt(mos, compression);
    Bytes.writeUInt(mos, columnIndex);
    Bytes.writeUInt(mos, maxRepetitionLevel);
    Bytes.writeUInt(mos, maxDefinitionLevel);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    ColumnSpec cs = (ColumnSpec) o;
    return type == cs.type &&
      encoding == cs.encoding &&
      compression == cs.compression &&
      maxRepetitionLevel == cs.maxRepetitionLevel &&
      maxDefinitionLevel == cs.maxDefinitionLevel;
  }

  public static void writeNullTo(MemoryOutputStream mos) {
    mos.write(NULL);
  }

  public static ColumnSpec read(ByteBuffer bb){
    if (bb.get() == NULL) {
      return null;
    }
    return new ColumnSpec(Bytes.readSInt(bb),
                          Bytes.readUInt(bb),
                          Bytes.readUInt(bb),
                          Bytes.readUInt(bb),
                          Bytes.readUInt(bb),
                          Bytes.readUInt(bb));
  }

}
