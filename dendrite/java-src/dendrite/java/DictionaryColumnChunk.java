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
import java.util.Iterator;
import java.util.List;

public final class DictionaryColumnChunk {

  public final static class Reader implements IColumnChunkReader {

    private final ByteBuffer bb;
    private final Metadata.ColumnChunk columnChunkMetadata;
    private final Types types;
    private final Schema.Column column;
    private final int partitionLength;

    public Reader(Types types, ByteBuffer bb, Metadata.ColumnChunk columnChunkMetadata,
                  Schema.Column column, int partitionLength) {
      this.types = types;
      this.bb = bb;
      this.columnChunkMetadata = columnChunkMetadata;
      this.column = column;
      this.partitionLength = partitionLength;
    }

    @Override
    public Iterator<List<Object>> iterator() {
      return Pages.readAndPartitionDataPagesWithDictionary(
         Bytes.sliceAhead(bb, columnChunkMetadata.dictionaryPageOffset),
         columnChunkMetadata.numDataPages,
         partitionLength,
         column.repetitionLevel,
         column.definitionLevel,
         types.getDecoderFactory(column.type, Types.PLAIN, column.fn),
         types.getDecoderFactory(Types.INT, Types.PACKED_RUN_LENGTH),
         types.getDecompressorFactory(column.compression)).iterator();
    }

    @Override
    public Iterable<IPageHeader> getPageHeaders() {
      return Pages.getHeaders(Bytes.sliceAhead(bb, columnChunkMetadata.dictionaryPageOffset),
                              1 + columnChunkMetadata.numDataPages);
    }

    @Override
    public Stats.ColumnChunk stats() {
      return Stats.createColumnChunkStats(Pages.getPagesStats(getPageHeaders()));
    }

    @Override
    public Metadata.ColumnChunk metadata() {
      return columnChunkMetadata;
    }

    @Override
    public Schema.Column column() {
      return column;
    }
  }

  public static final class Writer implements IColumnChunkWriter {

    private final Schema.Column column;
    private final Dictionary.Encoder dictEncoder;
    private final DictionaryPage.Writer dictPageWriter;
    private final DataColumnChunk.Writer indicesColumnChunkWriter;
    private final MemoryOutputStream mos;
    private double bytesPerDictionaryValue = -1.0;
    private int dictionaryHeaderLength = -1;

    private Writer(Types types, Schema.Column column, int targetDataPageLength) {
      this.dictEncoder = Dictionary.Encoder.create(column.type, Types.PACKED_RUN_LENGTH);
      Schema.Column indicesColumn = getIndicesColumn(column);
      DataPage.Writer indicesPageWriter = DataPage.Writer.create(column.repetitionLevel,
                                                                 column.definitionLevel,
                                                                 dictEncoder,
                                                                 null);
      this.indicesColumnChunkWriter
        = DataColumnChunk.Writer.create(indicesPageWriter, indicesColumn, targetDataPageLength);
      this.column = column;
      this.mos = new MemoryOutputStream();
      this.dictPageWriter = DictionaryPage.Writer.create(types.getEncoder(column.type, Types.PLAIN),
                                                         types.getCompressor(column.compression));
    }

    public static Writer create(Types types, Schema.Column column, int targetDataPageLength) {
      return new Writer(types, column, targetDataPageLength);
    }

    @Override
    public void write(Iterable<Object> values) {
      indicesColumnChunkWriter.write(values);
    }

    @Override
    public Schema.Column column() {
      return column;
    }

    @Override
    public ByteBuffer byteBuffer() {
      mos.reset();
      mos.write(this);
      return mos.byteBuffer();
    }

    @Override
    public int numDataPages() {
      return indicesColumnChunkWriter.numDataPages();
    }

    @Override
    public Metadata.ColumnChunk metadata() {
      finish();
      return new Metadata.ColumnChunk(length(),
                                      indicesColumnChunkWriter.metadata().numDataPages,
                                      dictionaryLength(),
                                      0);
    }

    private void updateDictionaryLengthEstimates() {
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
      dictEncoder.resetDictionary();
      dictPageWriter.reset();
      indicesColumnChunkWriter.reset();
    }

    @Override
    public int length() {
      finish();
      return dictionaryLength() + indicesColumnChunkWriter.length();
    }

    private int dictionaryLength() {
      return 1 + dictPageWriter.length();
    }

    @Override
    public int estimatedLength() {
      return estimatedDictionaryLength() + indicesColumnChunkWriter.estimatedLength();
    }

    private int estimatedDictionaryLength() {
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

    private void encodeDictionaryPage() {
      dictPageWriter.reset();
      for (Object v : dictEncoder.getDictionary()) {
        dictPageWriter.write(v);
      }
      dictPageWriter.finish();
    }

    static Schema.Column getIndicesColumn(Schema.Column column) {
      return new Schema.Column(column.repetition, column.repetitionLevel, column.definitionLevel,
                               Types.INT, Types.PACKED_RUN_LENGTH, Types.NONE, column.columnIndex, -1, null);
    }
  }
}
