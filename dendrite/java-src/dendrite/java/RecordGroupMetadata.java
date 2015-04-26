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
import clojure.lang.IPersistentCollection;
import clojure.lang.ISeq;
import clojure.lang.ITransientCollection;
import clojure.lang.PersistentList;
import clojure.lang.RT;
import clojure.lang.Util;

import java.nio.ByteBuffer;

public final class RecordGroupMetadata implements IWriteable {

  public final int length;
  public final int numRecords;
  public final ISeq columnChunksMetadata;

  public RecordGroupMetadata(int length, int numRecords, IPersistentCollection columnChunksMetadata) {
    this.length = length;
    this.numRecords = numRecords;
    this.columnChunksMetadata = RT.seq(columnChunksMetadata);
  }

  private void writeColumnChunksMetadataTo(MemoryOutputStream mos) {
    Bytes.writeUInt(mos, RT.count(columnChunksMetadata));
    for (ISeq s = columnChunksMetadata; s != null; s = s.next()) {
      mos.write((ColumnChunkMetadata)s.first());
    }
  }

  private static IPersistentCollection readColumnChunksMetadata(ByteBuffer bb) {
    int n = Bytes.readUInt(bb);
    if (n == 0) {
      return null;
    }
    ITransientCollection columnChunksMetadata = PersistentLinkedSeq.newEmptyTransient();
    for (int i=0; i<n; ++i) {
      columnChunksMetadata.conj(ColumnChunkMetadata.read(bb));
    }
    return columnChunksMetadata.persistent();
  }

  @Override
  public void writeTo(MemoryOutputStream mos) {
    Bytes.writeUInt(mos, length);
    Bytes.writeUInt(mos, numRecords);
    writeColumnChunksMetadataTo(mos);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    RecordGroupMetadata rgm = (RecordGroupMetadata) o;
    return length == rgm.length &&
      numRecords == rgm.numRecords &&
      Util.equiv(columnChunksMetadata, rgm.columnChunksMetadata);
  }

  public static RecordGroupMetadata read(ByteBuffer bb) {
    return new RecordGroupMetadata(Bytes.readUInt(bb),
                                   Bytes.readUInt(bb),
                                   readColumnChunksMetadata(bb));
  }

}
