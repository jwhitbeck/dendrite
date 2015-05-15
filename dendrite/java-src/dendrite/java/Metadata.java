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
import java.util.Arrays;

public final class Metadata {

  public final static class ColumnChunk implements IWriteable {

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
      if (o == null) {
        return false;
      }
      ColumnChunk cc = (ColumnChunk) o;
      return length == cc.length &&
        numDataPages == cc.numDataPages &&
        dataPageOffset == cc.dataPageOffset &&
        dictionaryPageOffset == cc.dictionaryPageOffset;
    }

    public static ColumnChunk read(ByteBuffer bb) {
      return new ColumnChunk(Bytes.readUInt(bb),
                             Bytes.readUInt(bb),
                             Bytes.readUInt(bb),
                             Bytes.readUInt(bb));
    }

  }

  public final static class RecordGroup implements IWriteable {

    public final int length;
    public final int numRecords;
    public final ColumnChunk[] columnChunks;

    public RecordGroup(int length, int numRecords, ColumnChunk[] columnChunks) {
      this.length = length;
      this.numRecords = numRecords;
      this.columnChunks = columnChunks;
    }

    private void writeColumnChunksTo(MemoryOutputStream mos) {
      Bytes.writeUInt(mos, columnChunks.length);
      for (int i=0; i<columnChunks.length; ++i) {
        mos.write(columnChunks[i]);
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
      Bytes.writeUInt(mos, numRecords);
      writeColumnChunksTo(mos);
    }

    @Override
    public boolean equals(Object o) {
      if (o == null) {
        return false;
      }
      RecordGroup rg = (RecordGroup) o;
      return length == rg.length &&
        numRecords == rg.numRecords &&
        Arrays.equals(columnChunks, rg.columnChunks);
    }

    public static RecordGroup read(ByteBuffer bb) {
      return new RecordGroup(Bytes.readUInt(bb),
                             Bytes.readUInt(bb),
                             readColumnChunks(bb));
    }

  }

  public final static class File implements IWriteable {

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
      for (int i=0; i<recordGroups.length; ++i) {
        mos.write(recordGroups[i]);
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
      for (int i=0; i<customTypes.length; ++i) {
        mos.write(customTypes[i]);
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
      if (o == null) {
        return false;
      }
      File f = (File)o;
      return Arrays.equals(recordGroups, f.recordGroups) &&
        schema.equals(f.schema) &&
        Arrays.equals(customTypes, f.customTypes) &&
        metadata.equals(f.metadata);
    }

    public static File read(ByteBuffer bb) {
      return new File(readRecordGroups(bb),
                      Schema.read(bb),
                      readCustomTypes(bb),
                      Bytes.readByteBuffer(bb));
    }

  }

}
