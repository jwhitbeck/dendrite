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

import clojure.lang.IMapEntry;
import clojure.lang.IPersistentCollection;
import clojure.lang.IPersistentMap;
import clojure.lang.ISeq;
import clojure.lang.Symbol;
import clojure.lang.RT;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;

public abstract class OptimizingColumnChunkWriter implements IColumnChunkWriter {

  final Schema.Column column;
  final DataColumnChunk.Writer plainColumnChunkWriter;
  final StatsEncoder statsEncoder;

  OptimizingColumnChunkWriter(DataColumnChunk.Writer plainColumnChunkWriter,
                              Schema.Column column, StatsEncoder statsEncoder) {
    this.plainColumnChunkWriter = plainColumnChunkWriter;
    this.column = column;
    this.statsEncoder = statsEncoder;
  }

  public static OptimizingColumnChunkWriter create(Types types, Schema.Column column,
                                                   int targetDataPageLength) {
    Schema.Column plainColumn = getPlainColumn(column);
    switch (types.getPrimitiveType(plainColumn.type)) {
    case Types.BOOLEAN: return BooleanColumnChunk.create(types, plainColumn, targetDataPageLength);
    default: return null;
    }
  }

  abstract int getBestEncoding();

  public IColumnChunkWriter optimize(Types types, IPersistentMap compressionThresholds) {
    plainColumnChunkWriter.finish();
    DataColumnChunk.Reader plainReader = new DataColumnChunk.Reader(types, plainColumnChunkWriter.byteBuffer(),
                                                                    plainColumnChunkWriter.metadata(),
                                                                    plainColumnChunkWriter.column());
    IPersistentMap plainStats = plainReader.stats();
    int bestEncoding = getBestEncoding();
    int bestCompression = getBestCompression(types, bestEncoding, plainReader, plainStats,
                                             compressionThresholds);
    IColumnChunkWriter optimizedWriter
      = ColumnChunks.createWriter(types,
                                  getColumnWithEncodingAndCompression(column, bestEncoding, bestCompression),
                                  plainColumnChunkWriter.targetDataPageLength);
    copyTo(plainReader, optimizedWriter);
    return optimizedWriter;
  }

  void copyTo(DataColumnChunk.Reader columnChunkReader, IColumnChunkWriter columnChunkWriter) {
    for (ISeq s = columnChunkReader.getPageReaders(); s != null; s = s.next()) {
      DataPage.Reader reader = (DataPage.Reader)s.first();
      columnChunkWriter.write((ChunkedPersistentList)reader.read());
    }
  }

  int getBestCompression(Types types, int bestEncoding, DataColumnChunk.Reader plainReader,
                         IPersistentMap plainStats, IPersistentMap compressionThresholds) {
    if (bestEncoding == Types.DICTIONARY || bestEncoding == Types.FREQUENCY) {
      return getDictionaryBestCompression(types, bestEncoding, plainStats, compressionThresholds);
    } else {
      return getRegularBestCompression(types, bestEncoding, plainReader, plainStats, compressionThresholds);
    }
  }

  int getRegularBestCompression(Types types, int encoding, DataColumnChunk.Reader plainReader,
                                IPersistentMap plainStats, IPersistentMap compressionThresholds) {
    IColumnChunkWriter writer
      = DataColumnChunk.Writer.create(types,
                                      getColumnWithEncodingAndCompression(column, encoding, Types.NONE),
                                      plainColumnChunkWriter.targetDataPageLength);
    copyAtLeastOnePage(plainReader, writer);
    writer.finish();
    ByteBuffer bb = writer.byteBuffer();
    DataPage.Header h = (DataPage.Header)Pages.readHeader(bb);
    int numFirstPageNonNilValues = (int)RT.get(h.stats(), Stats.NUM_NON_NIL_VALUES);
    int numColumnNonNilValues = (int)RT.get(plainStats, Stats.NUM_NON_NIL_VALUES);
    double nonNilValuesMultiplier = (double)numColumnNonNilValues / (double)numFirstPageNonNilValues;
    final ByteBuffer dataByteBuffer = bb.slice();
    dataByteBuffer.position(h.byteOffsetData());
    dataByteBuffer.limit(h.bodyLength());
    IWriteable dataBytes = new IWriteable() {
        public void writeTo(MemoryOutputStream mos) {
          mos.write(dataByteBuffer);
        }
      };
    int levelsLength = (int)RT.get(plainStats, Stats.REPETITION_LEVELS_LENGTH)
      + (int)RT.get(plainStats, Stats.DEFINITION_LEVELS_LENGTH);
    int bestCompression = Types.NONE;
    int noCompressionLength = levelsLength + (int)RT.get(plainStats, Stats.DATA_LENGTH);
    int bestLength = noCompressionLength;
    for (Object o : compressionThresholds.without(Types.NONE_SYM)) {
      IMapEntry e = (IMapEntry)o;
      int compression = types.getCompression((Symbol)e.key());
      double threshold = (double)e.val();
      ICompressor compressor = types.getCompressor(compression);
      compressor.compress(dataBytes);
      compressor.finish();
      int length = (int)(compressor.length() * nonNilValuesMultiplier) + levelsLength;
      double compressionRatio = (double)noCompressionLength / (double)length;
      if (compressionRatio >= threshold && length < bestLength) {
        bestLength = length;
        bestCompression = compression;
      }
    }
    return bestCompression;
  }

  void copyAtLeastOnePage(DataColumnChunk.Reader plainReader, IColumnChunkWriter columnChunkWriter) {
    ISeq dataPageReaders = plainReader.getPageReaders();
    while (dataPageReaders != null && columnChunkWriter.numDataPages() == 0) {
      DataPage.Reader reader = (DataPage.Reader)dataPageReaders.first();
      columnChunkWriter.write((ChunkedPersistentList)reader.read());
      dataPageReaders = dataPageReaders.next();
    }
  }

  int getDictionaryBestCompression(Types types, int encoding, IPersistentMap plainStats,
                                   IPersistentMap compressionThresholds) {
    int indicesDataLength;
    if (encoding == Types.DICTIONARY) {
      indicesDataLength = estimateDictionaryIndicesColumnLength(plainStats);
    } else {
      indicesDataLength = estimateFrequencyIndicesColumnLength(plainStats);
    }
    int indicesColumnLength = (int)RT.get(plainStats, Stats.REPETITION_LEVELS_LENGTH)
      + (int)RT.get(plainStats, Stats.DEFINITION_LEVELS_LENGTH)
      + indicesDataLength;
    IEncoder plainEncoder = types.getEncoder(column.type, Types.PLAIN);
    for (Object o : statsEncoder.getFrequencies().keySet()) {
      plainEncoder.encode(o);
    }
    plainEncoder.finish();
    int bestCompression = Types.NONE;
    int noCompressionLength = indicesColumnLength + plainEncoder.length();
    int bestLength = noCompressionLength;
    for (Object o : compressionThresholds.without(Types.NONE_SYM)) {
      IMapEntry e = (IMapEntry)o;
      int compression = types.getCompression((Symbol)e.key());
      double threshold = (double)e.val();
      ICompressor compressor = types.getCompressor(compression);
      compressor.compress(plainEncoder);
      compressor.finish();
      int length = indicesColumnLength + compressor.length();
      double compressionRatio = (double)noCompressionLength / (double)length;
      if (compressionRatio >= threshold && length < bestLength) {
        bestLength = length;
        bestCompression = compression;
      }
    }
    return bestCompression;
  }

  int estimateDictionaryIndicesColumnLength(IPersistentMap plainStats) {
    int numEntries = statsEncoder.getFrequencies().size();
    int width = Bytes.getNumUIntBytes(numEntries);
    int numNonNilValues = (int)plainStats.valAt(Stats.NUM_NON_NIL_VALUES);
    return numNonNilValues * width;
  }

  int estimateFrequencyIndicesColumnLength(IPersistentMap plainStats) {
    int indicesLength = 0;
    Integer[] frequencies = statsEncoder.getFrequencies().values().toArray(new Integer[]{});
    Arrays.sort(frequencies);
    for (int i=0; i < frequencies.length; ++i) {
      int freq = frequencies[frequencies.length - i - 1];
      indicesLength += freq * Bytes.getNumUIntBytes(freq);
    }
    return indicesLength;
  }

  @Override
  public void write(ChunkedPersistentList values){
    plainColumnChunkWriter.write(values);
  }

  static void incrementFrequency(HashMap<Object,Integer> frequencies, Object v) {
    Integer freq = frequencies.get(v);
    if (freq == null) {
      frequencies.put(v, 1);
    } else {
      frequencies.put(v, freq.intValue() + 1);
    }
  }

  @Override
  public Schema.Column column() {
    return column;
  }

  @Override
  public ByteBuffer byteBuffer() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int numDataPages() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ColumnChunkMetadata metadata() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void finish() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void reset() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int length() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int estimatedLength() {
    return plainColumnChunkWriter.estimatedLength();
  }

  @Override
  public void writeTo(MemoryOutputStream memoryOutputStream) {
    throw new UnsupportedOperationException();
  }

  static Schema.Column getPlainColumn(Schema.Column column) {
    return getColumnWithEncodingAndCompression(column, Types.PLAIN, Types.NONE);
  }

  static Schema.Column getColumnWithEncodingAndCompression(Schema.Column column, int encoding,
                                                           int compression) {
    return new Schema.Column(column.repetition, column.repetitionLevel, column.definitionLevel,
                             column.type, encoding, compression, column.columnIndex, null);
  }

  static class StatsEncoder implements IEncoder {

    final IEncoder encoder;
    final HashMap<Object, Integer> frequencies;
    final int maxDictionarySize;

    StatsEncoder(IEncoder encoder, int maxDictionarySize) {
      this.encoder = encoder;
      this.maxDictionarySize = maxDictionarySize;
      this.frequencies = new HashMap<Object, Integer>();
    }

    public HashMap<Object, Integer> getFrequencies() {
      return frequencies;
    }

    @Override
    public void encode(Object o) {
      if (frequencies.size() < maxDictionarySize) {
        incrementFrequency(frequencies, o);
      }
      encoder.encode(o);
    }

    @Override
    public int numEncodedValues() {
      return encoder.numEncodedValues();
    }

    @Override
    public int length() {
      return encoder.length();
    }

    @Override
    public int estimatedLength() {
      return encoder.estimatedLength();
    }

    @Override
    public void reset() {
      encoder.reset();
    }

    @Override
    public void finish() {
      encoder.finish();
    }

    @Override
    public void writeTo(MemoryOutputStream mos) {
      mos.write(encoder);
    }

  }

  static class BooleanColumnChunk extends OptimizingColumnChunkWriter {

    BooleanColumnChunk(DataColumnChunk.Writer plainColumnChunkWriter, Schema.Column column,
                       StatsEncoder statsEncoder) {
      super(plainColumnChunkWriter, column, statsEncoder);
    }

    public static BooleanColumnChunk create(Types types, Schema.Column column, int targetDataPageLength) {
      StatsEncoder statsEncoder = new StatsEncoder(types.getEncoder(column.type, column.encoding),
                                                   targetDataPageLength);
      DataPage.Writer statsPageWriter = DataPage.Writer.create(column.repetitionLevel,
                                                               column.definitionLevel,
                                                               statsEncoder,
                                                               null);
      DataColumnChunk.Writer plainColumnChunkWriter
        = DataColumnChunk.Writer.create(statsPageWriter, column, targetDataPageLength);
      return new BooleanColumnChunk(plainColumnChunkWriter, column, statsEncoder);
    }

    @Override
    int getBestEncoding() {
      Integer numTrue = statsEncoder.getFrequencies().get(true);
      Integer numFalse = statsEncoder.getFrequencies().get(false);
      if (numTrue == null || numFalse == null) {
        return Types.DICTIONARY;
      } else {
        if ( numTrue > 20 * numFalse || numFalse > 20 * numTrue) {
          return Types.DICTIONARY;
        } else {
          return Types.PLAIN;
        }
      }
    }
  }

}
