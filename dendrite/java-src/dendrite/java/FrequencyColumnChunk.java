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

public final class FrequencyColumnChunk {

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
          types.getDecoderFactory(Types.INT, Types.VLQ),
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

  public final static class Writer implements IColumnChunkWriter {

    private final Schema.Column column;
    private final Dictionary.Encoder dictEncoder;
    private final DictionaryPage.Writer dictPageWriter;
    private final DataColumnChunk.Writer tempIndicesColumnChunkWriter;
    private final DataColumnChunk.Writer frequencyIndicesColumnChunkWriter;
    private final MemoryOutputStream mos;
    private double bytesPerDictionaryValue = -1.0;
    private int dictionaryHeaderLength = -1;
    private boolean isFinished = false;

    private Writer(Types types, Schema.Column column, int targetDataPageLength) {
      this.dictEncoder = Dictionary.Encoder.create(column.type, Types.VLQ);
      Schema.Column indicesColumn = getIndicesColumn(column);
      DataPage.Writer indicesPageWriter = DataPage.Writer.create(column.repetitionLevel,
                                                                 column.definitionLevel,
                                                                 dictEncoder,
                                                                 null);
      this.tempIndicesColumnChunkWriter
        = DataColumnChunk.Writer.create(indicesPageWriter, indicesColumn, targetDataPageLength);
      this.frequencyIndicesColumnChunkWriter
        = DataColumnChunk.Writer.create(types, indicesColumn, targetDataPageLength);
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
      tempIndicesColumnChunkWriter.write(values);
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
      return tempIndicesColumnChunkWriter.numDataPages();
    }

    @Override
    public Metadata.ColumnChunk metadata() {
      finish();
      return new Metadata.ColumnChunk(length(),
                                      frequencyIndicesColumnChunkWriter.metadata().numDataPages,
                                      dictionaryLength(),
                                      0);
    }

    void updateDictionaryLengthEstimates() {
      IPageHeader h = dictPageWriter.header();
      bytesPerDictionaryValue
        = (int)((double)h.bodyLength() / (double)dictPageWriter.numValues());
      dictionaryHeaderLength = h.headerLength();
    }

    IDecoderFactory getFrequencyMappedIndicesDecoderFactory() {
      final int[] indicesByFrequency = dictEncoder.getIndicesByFrequency();
      final IDecoderFactory intDecoderFactory = Types.getPrimitiveDecoderFactory(Types.INT, Types.VLQ);
      return new ADecoderFactory() {
        public IDecoder create(ByteBuffer bb) {
          final IIntDecoder intDecoder = (IIntDecoder)intDecoderFactory.create(bb);
          return new IDecoder() {
            public Object decode() { return indicesByFrequency[intDecoder.decodeInt()]; }
            public int numEncodedValues() { return intDecoder.numEncodedValues(); }
          };
        }
      };
    }

    @Override
    public void finish() {
      if (!isFinished) {
        tempIndicesColumnChunkWriter.finish();
        Iterable<DataPage.Reader> dataPageReaders
          = Pages.getDataPageReaders(tempIndicesColumnChunkWriter.byteBuffer(),
                                     tempIndicesColumnChunkWriter.metadata().numDataPages,
                                     column.repetitionLevel,
                                     column.definitionLevel,
                                     getFrequencyMappedIndicesDecoderFactory(),
                                     null);
        for (DataPage.Reader reader : dataPageReaders) {
          frequencyIndicesColumnChunkWriter.write(reader);
        }
        frequencyIndicesColumnChunkWriter.finish();
        encodeDictionaryPage();
        updateDictionaryLengthEstimates();
        isFinished = true;
      }
    }

    @Override
    public void reset() {
      dictEncoder.reset();
      dictEncoder.resetDictionary();
      dictPageWriter.reset();
      tempIndicesColumnChunkWriter.reset();
      frequencyIndicesColumnChunkWriter.reset();
    }

    @Override
    public int length() {
      finish();
      return dictionaryLength() + frequencyIndicesColumnChunkWriter.length();
    }

    private int dictionaryLength() {
      return 1 + dictPageWriter.length();
    }

    @Override
    public int estimatedLength() {
      return estimatedDictionaryLength() + tempIndicesColumnChunkWriter.estimatedLength();
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
      memoryOutputStream.write(frequencyIndicesColumnChunkWriter);
    }

    private void encodeDictionaryPage() {
      dictPageWriter.reset();
      for (Object v : dictEncoder.getDictionaryByFrequency()) {
        dictPageWriter.write(v);
      }
      dictPageWriter.finish();
    }

    private static Schema.Column getIndicesColumn(Schema.Column column) {
      return new Schema.Column(column.repetition, column.repetitionLevel, column.definitionLevel,
                               Types.INT, Types.VLQ, Types.NONE, column.columnIndex, -1, null);
    }
  }
}
