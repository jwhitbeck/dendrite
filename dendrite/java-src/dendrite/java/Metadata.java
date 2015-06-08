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

import java.nio.ByteBuffer;
import java.util.Arrays;

public final class Metadata {

  public static final class ColumnChunk implements IWriteable {

    public final int length;
    public final int numDataPages;
    public final int dataPageOffset;
    public final int dictionaryPageOffset;

    public ColumnChunk(int length, int numDataPages, int dataPageOffset, int dictionaryPageOffset) {
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
      if (!(o instanceof ColumnChunk)) {
        return false;
      }
      ColumnChunk cc = (ColumnChunk) o;
      return length == cc.length
        && numDataPages == cc.numDataPages
        && dataPageOffset == cc.dataPageOffset
        && dictionaryPageOffset == cc.dictionaryPageOffset;
    }

    @Override
    public int hashCode() {
      throw new UnsupportedOperationException();
    }

    public static ColumnChunk read(ByteBuffer bb) {
      return new ColumnChunk(Bytes.readUInt(bb),
                             Bytes.readUInt(bb),
                             Bytes.readUInt(bb),
                             Bytes.readUInt(bb));
    }

  }

  public static final class RecordGroup implements IWriteable {

    public final int length;
    public final long numRecords;
    public final ColumnChunk[] columnChunks;

    public RecordGroup(int length, long numRecords, ColumnChunk[] columnChunks) {
      this.length = length;
      this.numRecords = numRecords;
      this.columnChunks = columnChunks;
    }

    private void writeColumnChunksTo(MemoryOutputStream mos) {
      Bytes.writeUInt(mos, columnChunks.length);
      for (ColumnChunk columnChunk : columnChunks) {
        mos.write(columnChunk);
      }
    }

    private static ColumnChunk[] readColumnChunks(ByteBuffer bb) {
      int n = Bytes.readUInt(bb);
      ColumnChunk[] columnChunks = new ColumnChunk[n];
      for (int i=0; i<n; ++i) {
        columnChunks[i] = ColumnChunk.read(bb);
      }
      return columnChunks;
    }

    @Override
    public void writeTo(MemoryOutputStream mos) {
      Bytes.writeUInt(mos, length);
      Bytes.writeULong(mos, numRecords);
      writeColumnChunksTo(mos);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof RecordGroup)) {
        return false;
      }
      RecordGroup rg = (RecordGroup) o;
      return length == rg.length
        && numRecords == rg.numRecords
        && Arrays.equals(columnChunks, rg.columnChunks);
    }

    @Override
    public int hashCode() {
      throw new UnsupportedOperationException();
    }

    public static RecordGroup read(ByteBuffer bb) {
      return new RecordGroup(Bytes.readUInt(bb),
                             Bytes.readULong(bb),
                             readColumnChunks(bb));
    }

  }

  public static final class File implements IWriteable {

    public final RecordGroup[] recordGroups;
    public final Schema schema;
    public final CustomType[] customTypes;
    public final ByteBuffer metadata;

    public File(RecordGroup[] recordGroups, Schema schema, CustomType[] customTypes, ByteBuffer metadata) {
      this.recordGroups = recordGroups;
      this.schema = schema;
      this.customTypes = customTypes;
      this.metadata = metadata;
    }

    private void writeRecordGroupsTo(MemoryOutputStream mos) {
      Bytes.writeUInt(mos, recordGroups.length);
      for (RecordGroup recordGroup : recordGroups) {
        mos.write(recordGroup);
      }
    }

    private static RecordGroup[] readRecordGroups(ByteBuffer bb) {
      int n = Bytes.readUInt(bb);
      RecordGroup[] recordGroups = new RecordGroup[n];
      for (int i=0; i<n; ++i) {
        recordGroups[i] = RecordGroup.read(bb);
      }
      return recordGroups;
    }

    private void writeCustomTypesTo(MemoryOutputStream mos) {
      Bytes.writeUInt(mos, customTypes.length);
      for (CustomType customType: customTypes) {
        mos.write(customType);
      }
    }

    private static CustomType[] readCustomTypes(ByteBuffer bb) {
      int n = Bytes.readUInt(bb);
      CustomType[] customTypes = new CustomType[n];
      for (int i=0; i<n; ++i) {
        customTypes[i] = CustomType.read(bb);
      }
      return customTypes;
    }

    @Override
    public void writeTo(MemoryOutputStream mos) {
      writeRecordGroupsTo(mos);
      Schema.writeTo(mos, schema);
      writeCustomTypesTo(mos);
      Bytes.writeByteBuffer(mos, metadata);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof File)) {
        return false;
      }
      File f = (File)o;
      return Arrays.equals(recordGroups, f.recordGroups)
        && schema.equals(f.schema)
        && Arrays.equals(customTypes, f.customTypes)
        && metadata.equals(f.metadata);
    }

    @Override
    public int hashCode() {
      throw new UnsupportedOperationException();
    }

    public static File read(ByteBuffer bb) {
      return new File(readRecordGroups(bb),
                      Schema.read(bb),
                      readCustomTypes(bb),
                      Bytes.readByteBuffer(bb));
    }

  }

}
