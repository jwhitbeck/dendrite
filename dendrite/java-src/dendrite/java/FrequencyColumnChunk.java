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

  public static final class Reader implements IColumnChunkReader {

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
          column.enclosingEmptyDefinitionLevel,
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
    public Stats.ColumnChunk getStats() {
      return Stats.createColumnChunkStats(Pages.getPagesStats(getPageHeaders()));
    }

    @Override
    public Metadata.ColumnChunk getMetadata() {
      return columnChunkMetadata;
    }

    @Override
    public Schema.Column getColumn() {
      return column;
    }

  }

  public static final class Writer implements IColumnChunkWriter {

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
      this.dictEncoder = Dictionary.Encoder.create(ColumnChunks.getType(types, column.type), Types.VLQ);
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
      this.dictPageWriter
        = DictionaryPage.Writer.create(types.getEncoder(ColumnChunks.getType(types, column.type), Types.PLAIN),
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
    public Schema.Column getColumn() {
      return column;
    }

    @Override
    public ByteBuffer toByteBuffer() {
      mos.reset();
      mos.write(this);
      return mos.toByteBuffer();
    }

    @Override
    public int getNumDataPages() {
      return tempIndicesColumnChunkWriter.getNumDataPages();
    }

    @Override
    public Metadata.ColumnChunk getMetadata() {
      finish();
      return new Metadata.ColumnChunk(getLength(),
                                      frequencyIndicesColumnChunkWriter.getMetadata().numDataPages,
                                      getDictionaryLength(),
                                      0);
    }

    private void updateDictionaryLengthEstimates() {
      IPageHeader h = dictPageWriter.getHeader();
      bytesPerDictionaryValue
        = (int)((double)h.getBodyLength() / (double)dictPageWriter.getNumValues());
      dictionaryHeaderLength = h.getHeaderLength();
    }

    private IDecoderFactory getFrequencyMappedIndicesDecoderFactory() {
      final int[] indicesByFrequency = dictEncoder.getIndicesByFrequency();
      final IDecoderFactory intDecoderFactory = Types.getPrimitiveDecoderFactory(Types.INT, Types.VLQ);
      return new ADecoderFactory() {
        public IDecoder create(ByteBuffer bb) {
          final IIntDecoder intDecoder = (IIntDecoder)intDecoderFactory.create(bb);
          return new IDecoder() {
            public Object decode() {
              return indicesByFrequency[intDecoder.decodeInt()];
            }

            public int getNumEncodedValues() {
              return intDecoder.getNumEncodedValues();
            }
          };
        }
      };
    }

    @Override
    public void finish() {
      if (!isFinished) {
        tempIndicesColumnChunkWriter.finish();
        Iterable<DataPage.Reader> dataPageReaders
          = Pages.getDataPageReaders(tempIndicesColumnChunkWriter.toByteBuffer(),
                                     tempIndicesColumnChunkWriter.getMetadata().numDataPages,
                                     column.repetitionLevel,
                                     column.definitionLevel,
                                     column.enclosingEmptyDefinitionLevel,
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
      isFinished = false;
    }

    @Override
    public int getLength() {
      finish();
      return getDictionaryLength() + frequencyIndicesColumnChunkWriter.getLength();
    }

    private int getDictionaryLength() {
      return 1 + dictPageWriter.getLength();
    }

    @Override
    public int getEstimatedLength() {
      return estimatedDictionaryLength() + tempIndicesColumnChunkWriter.getEstimatedLength();
    }

    private int estimatedDictionaryLength() {
      if (bytesPerDictionaryValue > 0) {
        return 1 + dictionaryHeaderLength
          + (int)(dictEncoder.getNumDictionaryValues() * bytesPerDictionaryValue);
      } else if (dictEncoder.getNumDictionaryValues() > 0) {
        encodeDictionaryPage();
        updateDictionaryLengthEstimates();
        return 1 + dictionaryHeaderLength
          + (int)(dictEncoder.getNumDictionaryValues() * bytesPerDictionaryValue);
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
      return new Schema.Column(column.presence, column.repetitionLevel, column.definitionLevel,
                               Types.INT, Types.VLQ, Types.NONE, column.columnIndex,
                               column.enclosingEmptyDefinitionLevel, -1, null);
    }
  }
}
