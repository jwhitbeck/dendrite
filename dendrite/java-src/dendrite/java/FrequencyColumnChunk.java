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

import clojure.lang.AFn;
import clojure.lang.ArraySeq;
import clojure.lang.IFn;
import clojure.lang.IPersistentCollection;
import clojure.lang.IPersistentMap;
import clojure.lang.ISeq;
import clojure.lang.RT;

import java.nio.ByteBuffer;

public final class FrequencyColumnChunk {

  public final static class Reader implements IColumnChunkReader {

    private final ByteBuffer bb;
    private final Metadata.ColumnChunk columnChunkMetadata;
    private final Types types;
    private final Schema.Column column;

    public Reader(Types types, ByteBuffer bb, Metadata.ColumnChunk columnChunkMetadata,
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
         types.getDecoderFactory(column.type, Types.PLAIN, column.fn),
         types.getDecoderFactory(Types.INT, Types.VLQ),
         types.getDecompressorFactory(column.compression));
    }

    @Override
    public ISeq getPageHeaders() {
      return Pages.readHeaders(Bytes.sliceAhead(bb, columnChunkMetadata.dictionaryPageOffset),
                               columnChunkMetadata.numDataPages);
    }

    @Override
    public IPersistentMap stats() {
      return Stats.columnChunkStats(Pages.getPagesStats(getPageHeaders()));
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

    final Schema.Column column;
    final Dictionary.Encoder dictEncoder;
    final DictionaryPage.Writer dictPageWriter;
    final DataColumnChunk.Writer tempIndicesColumnChunkWriter;
    final DataColumnChunk.Writer frequencyIndicesColumnChunkWriter;
    final MemoryOutputStream mos;
    double bytesPerDictionaryValue = -1.0;
    int dictionaryHeaderLength = -1;
    boolean isFinished = false;

    Writer(Types types, Schema.Column column, int targetDataPageLength) {
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
    public void write(ChunkedPersistentList values) {
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
        ISeq dataPageReaders
          = Pages.getDataPageReaders(tempIndicesColumnChunkWriter.byteBuffer(),
                                     tempIndicesColumnChunkWriter.metadata().numDataPages,
                                     column.repetitionLevel,
                                     column.definitionLevel,
                                     getFrequencyMappedIndicesDecoderFactory(),
                                     null);
        for (ISeq s = dataPageReaders; s != null; s = s.next()) {
          DataPage.Reader reader = (DataPage.Reader)s.first();
          ChunkedPersistentList values = (ChunkedPersistentList)reader.read();
          frequencyIndicesColumnChunkWriter.write(values);
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
      dictPageWriter.reset();
      tempIndicesColumnChunkWriter.reset();
      frequencyIndicesColumnChunkWriter.reset();
    }

    @Override
    public int length() {
      finish();
      return dictionaryLength() + frequencyIndicesColumnChunkWriter.length();
    }

    int dictionaryLength() {
      return dictPageWriter.length();
    }

    @Override
    public int estimatedLength() {
      return estimatedDictionaryLength() + tempIndicesColumnChunkWriter.estimatedLength();
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
      memoryOutputStream.write(frequencyIndicesColumnChunkWriter);
    }

    void encodeDictionaryPage() {
      dictPageWriter.reset();
      dictPageWriter.write(ArraySeq.create(dictEncoder.getDictionaryByFrequency()));
      dictPageWriter.finish();
    }

    static Schema.Column getIndicesColumn(Schema.Column column) {
      return new Schema.Column(column.repetition, column.repetitionLevel, column.definitionLevel,
                               Types.INT, Types.VLQ, Types.NONE, column.columnIndex, null);
    }
  }
}
