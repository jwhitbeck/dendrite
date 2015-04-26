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
import clojure.lang.IPersistentCollection;
import clojure.lang.ISeq;
import clojure.lang.ITransientCollection;
import clojure.lang.RT;
import clojure.lang.Util;

import java.nio.ByteBuffer;

public final class FileMetadata implements IWriteable {

  public final ISeq recordGroupsMetadata;
  public final Schema schema;
  public final ISeq customTypes;
  public final ByteBuffer metadata;

  public FileMetadata(IPersistentCollection recordGroupsMetadata, Schema schema,
                      IPersistentCollection customTypes, ByteBuffer metadata) {
    this.recordGroupsMetadata = RT.seq(recordGroupsMetadata);
    this.schema = schema;
    this.customTypes = RT.seq(customTypes);
    this.metadata = metadata;
  }

  private void writeRecordGroupsMetadataTo(MemoryOutputStream mos) {
    Bytes.writeUInt(mos, RT.count(recordGroupsMetadata));
    for (ISeq s = recordGroupsMetadata; s != null; s = s.next()) {
      mos.write((RecordGroupMetadata)s.first());
    }
  }

  private static IPersistentCollection readRecordGroupsMetadata(ByteBuffer bb) {
    int n = Bytes.readUInt(bb);
    if (n == 0) {
      return null;
    }
    ITransientCollection recordGroupsMetadata = PersistentLinkedSeq.newEmptyTransient();
    for (int i=0; i<n; ++i) {
      recordGroupsMetadata.conj(RecordGroupMetadata.read(bb));
    }
    return recordGroupsMetadata.persistent();
  }

  private void writeCustomTypesTo(MemoryOutputStream mos) {
    Bytes.writeUInt(mos, RT.count(customTypes));
    for (ISeq s = customTypes; s != null; s = s.next()) {
      mos.write((CustomType)s.first());
    }
  }

  private static IPersistentCollection readCustomTypes(ByteBuffer bb) {
    int n = Bytes.readUInt(bb);
    if (n == 0) {
      return null;
    }
    ITransientCollection customTypes = PersistentLinkedSeq.newEmptyTransient();
    for (int i=0; i<n; ++i) {
      customTypes.conj(CustomType.read(bb));
    }
    return customTypes.persistent();
  }

  @Override
  public void writeTo(MemoryOutputStream mos) {
    writeRecordGroupsMetadataTo(mos);
    mos.write(schema);
    writeCustomTypesTo(mos);
    Bytes.writeByteBuffer(mos, metadata);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    FileMetadata fm = (FileMetadata) o;
    return Util.equiv(recordGroupsMetadata, fm.recordGroupsMetadata) &&
      schema.equals(fm.schema) &&
      Util.equiv(customTypes, fm.customTypes) &&
      metadata.equals(fm.metadata);
  }

  public static FileMetadata read(ByteBuffer bb) {
    return new FileMetadata(readRecordGroupsMetadata(bb),
                            Schema.read(bb),
                            readCustomTypes(bb),
                            Bytes.readByteBuffer(bb));
  }

}
