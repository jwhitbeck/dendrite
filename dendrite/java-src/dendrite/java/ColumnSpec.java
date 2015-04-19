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
import clojure.lang.IFn;
import clojure.lang.IPersistentVector;
import clojure.lang.Util;

import java.nio.ByteBuffer;

public final class ColumnSpec implements IWriteable {

  public final Keyword type;
  public final int encoding;
  public final int compression;
  public final int columnIndex;
  public final int queryColumnIndex;
  public final int maxRepetitionLevel;
  public final int maxDefinitionLevel;
  public final IFn mapFn;
  public final IPersistentVector path;

  private final static int NULL = 0;
  private final static int PRESENT = 1;

  public ColumnSpec(Keyword type, int encoding, int compression, int columnIndex, int queryColumnIndex,
                    int maxRepetitionLevel, int maxDefinitionLevel, IFn mapFn, IPersistentVector path) {
    this.type = type;
    this.encoding = encoding;
    this.compression = compression;
    this.columnIndex = columnIndex;
    this.queryColumnIndex = queryColumnIndex;
    this.maxRepetitionLevel = maxRepetitionLevel;
    this.maxDefinitionLevel = maxDefinitionLevel;
    this.mapFn = mapFn;
    this.path = path;
  }

  @Override
  public void writeTo(MemoryOutputStream mos) {
    mos.write(PRESENT); // ColumSpec can be null. If so, the first byte will be 0;
    Bytes.writeByteArray(mos, Encodings.stringToUFT8Bytes(Encodings.keywordToString(type)));
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
    return Util.equiv(type, cs.type) &&
      encoding == cs.encoding &&
      compression == cs.compression &&
      queryColumnIndex == cs.queryColumnIndex &&
      maxRepetitionLevel == cs.maxRepetitionLevel &&
      maxDefinitionLevel == cs.maxDefinitionLevel &&
      mapFn == cs.mapFn &&
      Util.equiv(path, cs.path);
  }

  public static void writeNullTo(MemoryOutputStream mos) {
    mos.write(NULL);
  }

  public static ColumnSpec read(ByteBuffer bb){
    if (bb.get() == NULL) {
      return null;
    }
    return new ColumnSpec(Encodings.stringToKeyword(Encodings.UTF8BytesToString(Bytes.readByteArray(bb))),
                          Bytes.readUInt(bb),
                          Bytes.readUInt(bb),
                          Bytes.readUInt(bb),
                          0,
                          Bytes.readUInt(bb),
                          Bytes.readUInt(bb),
                          null,
                          null);
  }

}
