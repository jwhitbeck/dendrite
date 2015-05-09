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

import clojure.lang.ArraySeq;
import clojure.lang.IPersistentCollection;
import clojure.lang.ISeq;
import clojure.lang.RT;

import java.nio.ByteBuffer;

public final class DictionaryColumnChunk {

  public final static class Reader implements IColumnChunkReader {

    private final ByteBuffer bb;
    private final ColumnChunkMetadata columnChunkMetadata;
    private final Types types;
    private final Schema.Column column;

    public Reader(Types types, ByteBuffer bb, ColumnChunkMetadata columnChunkMetadata,
                  Schema.Column column) {
      this.types = types;
      this.bb = bb;
      this.columnChunkMetadata = columnChunkMetadata;
      this.column = column;
    }

    @Override
    public ISeq readPartitioned(int partitionLength) {
      return Pages.readDataPagesWithDictionaryPartitioned
        (Bytes.sliceAhead(bb, columnChunkMetadata.dictionaryPageOffset),
         columnChunkMetadata.numDataPages,
         partitionLength,
         column.repetitionLevel,
         column.definitionLevel,
         types.getDecoderFactory(column.type, Types.PLAIN),
         types.getDecoderFactory(Types.INT, Types.PACKED_RUN_LENGTH),
         types.getDecompressorFactory(column.compression),
         column.fn);
    }

    @Override
    public ISeq getPageHeaders() {
      return Pages.readHeaders(Bytes.sliceAhead(bb, columnChunkMetadata.dataPageOffset),
                               columnChunkMetadata.numDataPages);
    }

    @Override
    public ColumnChunkMetadata metadata() {
      return columnChunkMetadata;
    }

  }

  public final static class Writer implements IColumnChunkWriter {

    final Schema.Column column;
    final Dictionary.Encoder dictEncoder;
    final DictionaryPage.Writer dictPageWriter;
    final DataColumnChunk.Writer indicesColumnChunkWriter;
    double bytesPerDictionaryValue = -1.0;
    int dictionaryHeaderLength = -1;

    Writer(Types types, Schema.Column column, int targetDataPageLength) {
      this.dictEncoder = Dictionary.Encoder.create(column.type, Types.PACKED_RUN_LENGTH);
      Schema.Column indicesSchemaLeaf = getIndicesSchemaLeaf(column);
      DataPage.Writer indicesPageWriter = DataPage.Writer.create(column.repetitionLevel,
                                                                 column.definitionLevel,
                                                                 dictEncoder,
                                                                 null);
      this.indicesColumnChunkWriter
        = DataColumnChunk.Writer.create(indicesPageWriter, column, targetDataPageLength);
      this.column = column;
      this.dictPageWriter = DictionaryPage.Writer.create(types.getEncoder(column.type, Types.PLAIN),
                                                         types.getCompressor(column.compression));
    }

    public static Writer create(Types types, Schema.Column column, int targetDataPageLength) {
      return new Writer(types, column, targetDataPageLength);
    }

    @Override
    public void write(ChunkedPersistentList values) {
      indicesColumnChunkWriter.write(values);
    }

    @Override
    public Schema.Column column() {
      return column;
    }

    @Override
    public ColumnChunkMetadata metadata() {
      return new ColumnChunkMetadata(length(),
                                     indicesColumnChunkWriter.metadata().numDataPages,
                                     dictionaryLength(),
                                     0);
    }

    @Override
    public IColumnChunkWriter optimize() {
      return this;
    }

    void updateDictionaryLengthEstimates() {
      IPageHeader h = dictPageWriter.header();
      bytesPerDictionaryValue
        = (int)((double)h.bodyLength() / (double)dictPageWriter.numValues());
      dictionaryHeaderLength = h.headerLength();
    }

    @Override
    public void finish() {
      encodeDictionaryPage();
      updateDictionaryLengthEstimates();
      indicesColumnChunkWriter.finish();
    }

    @Override
    public void reset() {
      dictEncoder.reset();
      dictPageWriter.reset();
      indicesColumnChunkWriter.reset();
    }

    @Override
    public int length() {
      return dictionaryLength() + indicesColumnChunkWriter.length();
    }

    int dictionaryLength() {
      return dictPageWriter.length();
    }

    @Override
    public int estimatedLength() {
      return estimatedDictionaryLength() + indicesColumnChunkWriter.estimatedLength();
    }

    int estimatedDictionaryLength() {
      if (bytesPerDictionaryValue > 0) {
        return 1 + dictionaryHeaderLength + (int)(dictEncoder.numDictionaryValues() * bytesPerDictionaryValue);
      } else if (dictEncoder.numDictionaryValues() > 0) {
        encodeDictionaryPage();
        updateDictionaryLengthEstimates();
        return 1 + dictionaryHeaderLength + (int)(dictEncoder.numDictionaryValues() * bytesPerDictionaryValue);
      } else {
        return 1;
      }
    }

    @Override
    public void writeTo(MemoryOutputStream memoryOutputStream) {
      finish();
      Pages.writeTo(memoryOutputStream, dictPageWriter);
      memoryOutputStream.write(indicesColumnChunkWriter);
    }

    void encodeDictionaryPage() {
      dictPageWriter.reset();
      dictPageWriter.write(ArraySeq.create(dictEncoder.getDictionary()));
      dictPageWriter.finish();
    }

    static Schema.Column getIndicesSchemaLeaf(Schema.Column column) {
      return new Schema.Column(column.repetition, column.repetitionLevel, column.definitionLevel,
                               Types.INT, Types.PACKED_RUN_LENGTH, Types.NONE, column.columnIndex, null);
    }
  }
}