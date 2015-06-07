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
import clojure.lang.IFn;
import clojure.lang.Symbol;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.HashMap;
import java.util.Map;

public abstract class OptimizingColumnChunkWriter implements IColumnChunkWriter {

  final Schema.Column column;
  final Schema.Column primitiveColumn;
  final DataColumnChunk.Writer plainColumnChunkWriter;
  final StatsCollector statsCollector;
  final Types types;

  OptimizingColumnChunkWriter(Types types, DataColumnChunk.Writer plainColumnChunkWriter,
                              Schema.Column column, StatsCollector statsCollector) {
    this.types = types;
    this.plainColumnChunkWriter = plainColumnChunkWriter;
    this.column = column;
    this.primitiveColumn = getPrimitiveColumn(types, column);
    this.statsCollector = statsCollector;
  }

  public static OptimizingColumnChunkWriter create(Types types, Schema.Column column,
                                                   int targetDataPageLength) {
    Schema.Column plainColumn = getPlainColumn(types, column);
    int primitiveType = types.getPrimitiveType(plainColumn.type);
    final StatsCollector statsCollector;
    switch (primitiveType) {
    case Types.INT:
      statsCollector = new IntStatsCollector(targetDataPageLength); break;
    case Types.LONG:
      statsCollector = new LongStatsCollector(targetDataPageLength); break;
    case Types.BYTE_ARRAY:
      statsCollector = new ByteArrayStatsCollector(targetDataPageLength); break;
    case Types.FIXED_LENGTH_BYTE_ARRAY:
      statsCollector = new ByteArrayStatsCollector(targetDataPageLength); break;
    default: statsCollector = new StatsCollector(targetDataPageLength);
    }
    DataPage.Writer statsPageWriter
      = DataPage.Writer.create(column.repetitionLevel,
                               column.definitionLevel,
                               types.getEncoder(plainColumn.type, plainColumn.encoding,
                                                getShim(statsCollector)),
                               null);
    DataColumnChunk.Writer plainColumnChunkWriter
      = DataColumnChunk.Writer.create(statsPageWriter, column, targetDataPageLength);
    switch (primitiveType) {
    case Types.BOOLEAN:
      return new BooleanColumnChunk(types, plainColumnChunkWriter, plainColumn, statsCollector);
    case Types.INT:
      return new IntColumnChunk(types, plainColumnChunkWriter, plainColumn, statsCollector);
    case Types.LONG:
      return new LongColumnChunk(types, plainColumnChunkWriter, plainColumn, statsCollector);
    case Types.FLOAT:
      return new DefaultColumnChunk(types, plainColumnChunkWriter, plainColumn, statsCollector);
    case Types.DOUBLE:
      return new DefaultColumnChunk(types, plainColumnChunkWriter, plainColumn, statsCollector);
    case Types.BYTE_ARRAY:
      return new ByteArrayColumnChunk(types, plainColumnChunkWriter, plainColumn, statsCollector);
    case Types.FIXED_LENGTH_BYTE_ARRAY:
      return new DefaultColumnChunk(types, plainColumnChunkWriter, plainColumn, statsCollector);
    default: return null; // Never reached
    }
  }

  abstract int getBestEncoding(DataColumnChunk.Reader plainReader, Stats.ColumnChunk plainStats);

  private static final int PARTITION_LENGTH = 100;

  public IColumnChunkWriter optimize(Map<Symbol,Double> compressionThresholds) {
    plainColumnChunkWriter.finish();
    DataColumnChunk.Reader primitiveReader
      = new DataColumnChunk.Reader(types, plainColumnChunkWriter.toByteBuffer(),
                                   plainColumnChunkWriter.getMetadata(),
                                   primitiveColumn, PARTITION_LENGTH);
    Stats.ColumnChunk plainStats = primitiveReader.getStats();
    int bestEncoding = getBestEncoding(primitiveReader, plainStats);
    int bestCompression = getBestCompression(bestEncoding, primitiveReader, plainStats, compressionThresholds);
    DataColumnChunk.Reader plainReader
      = new DataColumnChunk.Reader(types, plainColumnChunkWriter.toByteBuffer(),
                                   plainColumnChunkWriter.getMetadata(),
                                   column, PARTITION_LENGTH);
    IColumnChunkWriter optimizedWriter
      = ColumnChunks.createWriter(types,
                                  getColumnWith(types, column, column.type, bestEncoding, bestCompression),
                                  plainColumnChunkWriter.targetDataPageLength);
    copyTo(plainReader, optimizedWriter);
    return optimizedWriter;
  }

  private void copyTo(DataColumnChunk.Reader columnChunkReader, IColumnChunkWriter columnChunkWriter) {
    for (DataPage.Reader reader : columnChunkReader.getPageReaders()) {
      columnChunkWriter.write(reader);
    }
  }

  private int getBestCompression(int bestEncoding, DataColumnChunk.Reader primitiveReader,
                                 Stats.ColumnChunk plainStats, Map<Symbol,Double> compressionThresholds) {
    if (bestEncoding == Types.DICTIONARY || bestEncoding == Types.FREQUENCY) {
      return getDictionaryBestCompression(bestEncoding, plainStats, compressionThresholds);
    } else {
      return getRegularBestCompression(bestEncoding, primitiveReader, plainStats, compressionThresholds);
    }
  }

  private int getRegularBestCompression(int encoding, DataColumnChunk.Reader primitiveReader,
                                        Stats.ColumnChunk plainStats,
                                        Map<Symbol,Double> compressionThresholds) {
    IColumnChunkWriter writer
      = DataColumnChunk.Writer.create(types,
                                      primitiveColumn.withEncoding(encoding),
                                      plainColumnChunkWriter.targetDataPageLength);
    copyAtLeastOnePage(primitiveReader, writer);
    writer.finish();
    ByteBuffer bb = writer.toByteBuffer();
    DataPage.Header h = (DataPage.Header)Pages.readHeader(bb);
    double nonNilValuesMultiplier = (double)plainStats.numNonNilValues / (double)h.getStats().numNonNilValues;
    final ByteBuffer dataByteBuffer = bb.slice();
    dataByteBuffer.position(h.getByteOffsetData());
    dataByteBuffer.limit(h.getBodyLength());
    IWriteable dataBytes = new IWriteable() {
        public void writeTo(MemoryOutputStream mos) {
          mos.write(dataByteBuffer);
        }
      };
    int levelsLength = estimateLevelsLength(plainStats);
    int bestCompression = Types.NONE;
    int noCompressionLength = levelsLength + (int)(h.getUncompressedDataLength() * nonNilValuesMultiplier);
    int bestLength = noCompressionLength;
    for (Map.Entry<Symbol,Double> e : compressionThresholds.entrySet()) {
      int compression = types.getCompression(e.getKey());
      if (compression == Types.NONE) {
        continue;
      }
      double threshold = e.getValue();
      ICompressor compressor = types.getCompressor(compression);
      compressor.compress(dataBytes);
      compressor.finish();
      int length = (int)(compressor.getLength() * nonNilValuesMultiplier) + levelsLength;
      double compressionRatio = (double)noCompressionLength / (double)length;
      if (compressionRatio >= threshold && length < bestLength) {
        bestLength = length;
        bestCompression = compression;
      }
    }
    return bestCompression;
  }

  private void copyAtLeastOnePage(DataColumnChunk.Reader primitiveReader,
                                  IColumnChunkWriter columnChunkWriter) {
    Iterator<DataPage.Reader> i = primitiveReader.getPageReaders().iterator();
    while (i.hasNext() && columnChunkWriter.getNumDataPages() == 0) {
      columnChunkWriter.write(i.next());
    }
  }

  private int getDictionaryBestCompression(int encoding, Stats.ColumnChunk plainStats,
                                           Map<Symbol,Double> compressionThresholds) {
    int indicesDataLength;
    if (encoding == Types.DICTIONARY) {
      indicesDataLength = estimateDictionaryIndicesColumnLength(plainStats);
    } else {
      indicesDataLength = estimateFrequencyIndicesColumnLength(plainStats);
    }
    int indicesColumnLength = estimateLevelsLength(plainStats) + indicesDataLength;
    IEncoder primitiveEncoder = types.getEncoder(primitiveColumn.type, Types.PLAIN);
    for (Object o : statsCollector.getFrequencies().keySet()) {
      primitiveEncoder.encode(o);
    }
    primitiveEncoder.finish();
    int bestCompression = Types.NONE;
    int noCompressionLength = indicesColumnLength + primitiveEncoder.getLength();
    int bestLength = noCompressionLength;
    for (Map.Entry<Symbol,Double> e : compressionThresholds.entrySet()) {
      int compression = types.getCompression(e.getKey());
      if (compression == Types.NONE) {
        continue;
      }
      double threshold = e.getValue();
      ICompressor compressor = types.getCompressor(compression);
      compressor.compress(primitiveEncoder);
      compressor.finish();
      int length = indicesColumnLength + compressor.getLength();
      double compressionRatio = (double)noCompressionLength / (double)length;
      if (compressionRatio >= threshold && length < bestLength) {
        bestLength = length;
        bestCompression = compression;
      }
    }
    return bestCompression;
  }

  private int estimateLevelsLength(Stats.ColumnChunk plainStats) {
    return (int)(plainStats.repetitionLevelsLength + plainStats.definitionLevelsLength);
  }

  int estimateDictionaryIndicesColumnLength(Stats.ColumnChunk plainStats) {
    int numEntries = statsCollector.getFrequencies().size();
    int width = Bytes.getBitWidth(numEntries);
    return (int)((plainStats.numNonNilValues * width) / 8);
  }

  int estimateFrequencyIndicesColumnLength(Stats.ColumnChunk plainStats) {
    int indicesLength = 0;
    Integer[] frequencies = statsCollector.getFrequencies().values().toArray(new Integer[]{});
    Arrays.sort(frequencies);
    for (int i=0; i < frequencies.length; ++i) {
      int freq = frequencies[frequencies.length - i - 1];
      indicesLength += freq * Bytes.getNumUIntBytes(freq);
    }
    return indicesLength;
  }

  @Override
  public void write(Iterable<Object> values){
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
  public Schema.Column getColumn() {
    return column;
  }

  @Override
  public ByteBuffer toByteBuffer() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getNumDataPages() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Metadata.ColumnChunk getMetadata() {
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
  public int getLength() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getEstimatedLength() {
    return plainColumnChunkWriter.getEstimatedLength();
  }

  @Override
  public void writeTo(MemoryOutputStream memoryOutputStream) {
    throw new UnsupportedOperationException();
  }

  private static Schema.Column getPlainColumn(Types types, Schema.Column column) {
    int plainEncoding;
    if (types.getPrimitiveType(column.type) == Types.BYTE_ARRAY) {
      plainEncoding = Types.DELTA_LENGTH;
    } else {
      plainEncoding = Types.PLAIN;
    }
    return getColumnWith(types, column, column.type, plainEncoding, Types.NONE);
  }

  private static Schema.Column getPrimitiveColumn(Types types, Schema.Column column) {
    return getColumnWith(types, column, types.getPrimitiveType(column.type), column.encoding,
                         column.compression);
  }

  private static Schema.Column getColumnWith(Types types, Schema.Column column, int type, int encoding,
                                             int compression) {
    return new Schema.Column(column.repetition, column.repetitionLevel, column.definitionLevel,
                             type, encoding, compression, column.columnIndex, -1, null);
  }

  private static class StatsCollector {

    final HashMap<Object, Integer> frequencies;
    final int maxDictionarySize;

    private StatsCollector(int maxDictionarySize) {
      this.maxDictionarySize = maxDictionarySize;
      this.frequencies = new HashMap<Object, Integer>();
    }

    Map<Object, Integer> getFrequencies() {
      return frequencies;
    }

    boolean isDictionarySaturated() {
      return frequencies.size() == maxDictionarySize;
    }

    void process(Object o) {
      if (frequencies.size() < maxDictionarySize) {
        incrementFrequency(frequencies, o);
      }
    }
  }

  private static IFn getShim(final StatsCollector statsCollector) {
    return new AFn() {
      public Object invoke(Object o) {
        statsCollector.process(o);
        return o;
      }
    };
  }

  private static final class BooleanColumnChunk extends OptimizingColumnChunkWriter {

    private BooleanColumnChunk(Types types, DataColumnChunk.Writer plainColumnChunkWriter,
                               Schema.Column column, StatsCollector statsCollector) {
      super(types, plainColumnChunkWriter, column, statsCollector);
    }

    @Override
    int getBestEncoding(DataColumnChunk.Reader primitiveReader, Stats.ColumnChunk plainStats) {
      Integer numTrue = statsCollector.getFrequencies().get(true);
      Integer numFalse = statsCollector.getFrequencies().get(false);
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

  IOutputBuffer getEncodedDictionary() {
    IEncoder encoder = Types.getPrimitiveEncoder(primitiveColumn.type, Types.PLAIN);
    for (Object o : statsCollector.getFrequencies().keySet()) {
      encoder.encode(o);
    }
    encoder.finish();
    return encoder;
  }

  boolean isDictionaryTooLarge(IOutputBuffer encodedDictionary) {
    int targetDataPageLength = plainColumnChunkWriter.targetDataPageLength;
    int unCompressedLength = encodedDictionary.getLength();
    if (unCompressedLength < targetDataPageLength) {
      return false;
    } else {
      ICompressor compressor = types.getCompressor(Types.DEFLATE);
      compressor.compress(encodedDictionary);
      compressor.finish();
      return compressor.getLength() > targetDataPageLength;
    }
  }

  int estimateDataLength(int encoding, DataColumnChunk.Reader primitiveReader, int numNonNilValues) {
    IPageWriter pageWriter
      = DataPage.Writer.create(primitiveColumn.repetitionLevel,
                               primitiveColumn.definitionLevel,
                               types.getPrimitiveEncoder(primitiveColumn.type, encoding),
                               types.getCompressor(Types.NONE));
    DataPage.Reader pageReader = primitiveReader.getPageReaders().iterator().next();
    for (Object o : pageReader) {
      pageWriter.write(o);
    }
    pageWriter.finish();
    Stats.Page pageStats = pageWriter.getHeader().getStats();
    int numNonNilValuesInPage = (int)pageStats.numNonNilValues;
    double mult = ((double)numNonNilValues/(double)numNonNilValuesInPage);
    return (int)(pageStats.dataLength * mult);
  }

  private static final class DefaultColumnChunk extends OptimizingColumnChunkWriter {

    private DefaultColumnChunk(Types types, DataColumnChunk.Writer plainColumnChunkWriter,
                               Schema.Column column, StatsCollector statsCollector) {
      super(types, plainColumnChunkWriter, column, statsCollector);
    }

    @Override
    int getBestEncoding(DataColumnChunk.Reader primitiveReader, Stats.ColumnChunk plainStats) {
      if (statsCollector.isDictionarySaturated()) {
        return Types.PLAIN;
      }
      IOutputBuffer encodedDictionary = getEncodedDictionary();
      if (isDictionaryTooLarge(encodedDictionary)) {
        return Types.PLAIN;
      }
      int bestLength = (int)plainStats.dataLength;
      int bestEncoding = Types.PLAIN;
      int dictionaryLength = estimateDictionaryIndicesColumnLength(plainStats) + encodedDictionary.getLength();
      if (dictionaryLength < bestLength) {
        bestLength = dictionaryLength;
        bestEncoding = Types.DICTIONARY;
      }
      int frequencyLength = estimateFrequencyIndicesColumnLength(plainStats) + encodedDictionary.getLength();
      if (frequencyLength < bestLength) {
        bestEncoding = Types.FREQUENCY;
      }
      return bestEncoding;
    }
  }

  private static final class IntStatsCollector extends StatsCollector {

    private int minValue = Integer.MAX_VALUE;
    private int maxValue = Integer.MIN_VALUE;

    private IntStatsCollector(int maxDictionarySize) {
      super(maxDictionarySize);
    }

    @Override
    void process(Object o) {
      super.process(o);
      int v = (Integer)o;
      if (v > maxValue) {
        maxValue = v;
      }
      if (v < minValue) {
        minValue = v;
      }
    }
  }

  private static class IntColumnChunk extends OptimizingColumnChunkWriter {

    private final IntStatsCollector intStatsCollector;

    private IntColumnChunk(Types types, DataColumnChunk.Writer plainColumnChunkWriter, Schema.Column column,
                           StatsCollector statsCollector) {
      super(types, plainColumnChunkWriter, column, statsCollector);
      this.intStatsCollector = (IntStatsCollector)statsCollector;
    }

    private int getVLQDataLength() {
      int length = 0;
      for (Map.Entry<Object, Integer> e : statsCollector.getFrequencies().entrySet()) {
        int v = (Integer)e.getKey();
        int freq = e.getValue();
        length += Bytes.getNumUIntBytes(v) * freq;
      }
      return length;
    }

    private int getZigZagDataLength() {
      int length = 0;
      for (Map.Entry<Object, Integer> e : statsCollector.getFrequencies().entrySet()) {
        int v = Bytes.encodeZigZag32((Integer)e.getKey());
        int freq = e.getValue();
        length += Bytes.getNumUIntBytes(v) * freq;
      }
      return length;
    }

    @Override
    int getBestEncoding(DataColumnChunk.Reader primitiveReader, Stats.ColumnChunk plainStats) {
      int bestLength = (int)plainStats.dataLength;
      int bestEncoding = Types.PLAIN;
      int numNonNilValues = (int)plainStats.numNonNilValues;

      if (intStatsCollector.minValue >= 0) {
        int vlqLength;
        if (statsCollector.isDictionarySaturated()) {
          vlqLength = estimateDataLength(Types.VLQ, primitiveReader, numNonNilValues);
        } else {
          vlqLength = getVLQDataLength();
        }
        if (vlqLength < bestLength) {
          bestLength = vlqLength;
          bestEncoding = Types.VLQ;
        }
      } else {
        int zigzagLength;
        if (statsCollector.isDictionarySaturated()) {
          zigzagLength = estimateDataLength(Types.ZIG_ZAG, primitiveReader, numNonNilValues);
        } else {
          zigzagLength = getZigZagDataLength();
        }
        if (zigzagLength < bestLength) {
          bestLength = zigzagLength;
          bestEncoding = Types.ZIG_ZAG;
        }
      }

      if (intStatsCollector.minValue >= 0) {
        int packedRunLengthLength = (numNonNilValues * Bytes.getBitWidth(intStatsCollector.maxValue)) / 8;
        if (packedRunLengthLength < bestLength) {
          bestLength = packedRunLengthLength;
          bestEncoding = Types.PACKED_RUN_LENGTH;
        }
      }

      int deltaLength = estimateDataLength(Types.DELTA, primitiveReader, numNonNilValues);
      if (deltaLength < bestLength) {
        bestLength = deltaLength;
        bestEncoding = Types.DELTA;
      }

      if (!statsCollector.isDictionarySaturated()) {
        IOutputBuffer encodedDictionary = getEncodedDictionary();
        if (!isDictionaryTooLarge(encodedDictionary)) {
          int dictionaryLength = estimateDictionaryIndicesColumnLength(plainStats)
            + encodedDictionary.getLength();
          if (dictionaryLength < bestLength) {
            bestLength = dictionaryLength;
            bestEncoding = Types.DICTIONARY;
          }
          int frequencyLength = estimateFrequencyIndicesColumnLength(plainStats)
            + encodedDictionary.getLength();
          if (frequencyLength < bestLength) {
            bestLength = frequencyLength;
            bestEncoding = Types.FREQUENCY;
          }
        }

      }

      return bestEncoding;
    }
  }

  private static final class LongStatsCollector extends StatsCollector {

    private long minValue = Long.MAX_VALUE;
    private long maxValue = Long.MIN_VALUE;

    private LongStatsCollector(int maxDictionarySize) {
      super(maxDictionarySize);
    }

    @Override
    void process(Object o) {
      super.process(o);
      long v = (Long)o;
      if (v > maxValue) {
        maxValue = v;
      }
      if (v < minValue) {
        minValue = v;
      }
    }
  }

  private static final class LongColumnChunk extends OptimizingColumnChunkWriter {

    private final LongStatsCollector longStatsCollector;

    LongColumnChunk(Types types, DataColumnChunk.Writer plainColumnChunkWriter, Schema.Column column,
                   StatsCollector statsCollector) {
      super(types, plainColumnChunkWriter, column, statsCollector);
      this.longStatsCollector = (LongStatsCollector)statsCollector;
    }

    private int getVLQDataLength() {
      int length = 0;
      for (Map.Entry<Object, Integer> e : statsCollector.getFrequencies().entrySet()) {
        long v = (Long)e.getKey();
        int freq = e.getValue();
        length += Bytes.getNumULongBytes(v) * freq;
      }
      return length;
    }

    private int getZigZagDataLength() {
      int length = 0;
      for (Map.Entry<Object, Integer> e : statsCollector.getFrequencies().entrySet()) {
        long v = Bytes.encodeZigZag64((Long)e.getKey());
        int freq = e.getValue();
        length += Bytes.getNumULongBytes(v) * freq;
      }
      return length;
    }

    @Override
    int getBestEncoding(DataColumnChunk.Reader primitiveReader, Stats.ColumnChunk plainStats) {
      int bestLength = (int)plainStats.dataLength;
      int bestEncoding = Types.PLAIN;
      int numNonNilValues = (int)plainStats.numNonNilValues;

      if (longStatsCollector.minValue >= 0) {
        int vlqLength;
        if (statsCollector.isDictionarySaturated()) {
          vlqLength = estimateDataLength(Types.VLQ, primitiveReader, numNonNilValues);
        } else {
          vlqLength = getVLQDataLength();
        }
        if (vlqLength < bestLength) {
          bestLength = vlqLength;
          bestEncoding = Types.VLQ;
        }
      } else {
        int zigzagLength;
        if (statsCollector.isDictionarySaturated()) {
          zigzagLength = estimateDataLength(Types.ZIG_ZAG, primitiveReader, numNonNilValues);
        } else {
          zigzagLength = getZigZagDataLength();
        }
        if (zigzagLength < bestLength) {
          bestLength = zigzagLength;
          bestEncoding = Types.ZIG_ZAG;
        }
      }

      int deltaLength = estimateDataLength(Types.DELTA, primitiveReader, numNonNilValues);
      if (deltaLength < bestLength) {
        bestLength = deltaLength;
        bestEncoding = Types.DELTA;
      }

      if (!statsCollector.isDictionarySaturated()) {
        IOutputBuffer encodedDictionary = getEncodedDictionary();
        if (!isDictionaryTooLarge(encodedDictionary)) {
          int dictionaryLength = estimateDictionaryIndicesColumnLength(plainStats)
            + encodedDictionary.getLength();
          if (dictionaryLength < bestLength) {
            bestLength = dictionaryLength;
            bestEncoding = Types.DICTIONARY;
          }
          int frequencyLength = estimateFrequencyIndicesColumnLength(plainStats)
            + encodedDictionary.getLength();
          if (frequencyLength < bestLength) {
            bestLength = frequencyLength;
            bestEncoding = Types.FREQUENCY;
          }
        }

      }

      return bestEncoding;
    }
  }

  private static final class ByteArrayStatsCollector extends StatsCollector {

    private Map<Object, Integer> unwrappedFrequencies = null;

    private ByteArrayStatsCollector(int maxDictionarySize) {
      super(maxDictionarySize);
    }

    @Override
    void process(Object o) {
      if (frequencies.size() < maxDictionarySize) {
        incrementFrequency(frequencies, new HashableByteArray((byte[])o));
      }
    }

    @Override
    Map<Object, Integer> getFrequencies() {
      if (unwrappedFrequencies == null) {
        unwrappedFrequencies = new HashMap<Object, Integer>();
        for (Map.Entry<Object, Integer> e : frequencies.entrySet()) {
          unwrappedFrequencies.put(((HashableByteArray)e.getKey()).array, e.getValue());
        }
      }
      return unwrappedFrequencies;
    }

  }

  private static class ByteArrayColumnChunk extends OptimizingColumnChunkWriter {

    private ByteArrayColumnChunk(Types types, DataColumnChunk.Writer plainColumnChunkWriter,
                                 Schema.Column column, StatsCollector statsCollector) {
      super(types, plainColumnChunkWriter, column, statsCollector);
    }

    @Override
    int getBestEncoding(DataColumnChunk.Reader primitiveReader, Stats.ColumnChunk plainStats) {
      int plainLength = (int)plainStats.dataLength;
      int bestLength = plainLength;
      int bestEncoding = Types.DELTA_LENGTH;
      int numNonNilValues = (int)plainStats.numNonNilValues;

      int incrementalLength = estimateDataLength(Types.INCREMENTAL, primitiveReader, numNonNilValues);
      if (incrementalLength < plainLength) {
        bestLength = incrementalLength;
        bestEncoding = Types.INCREMENTAL;
      }

      if (!statsCollector.isDictionarySaturated()) {
        IOutputBuffer encodedDictionary = getEncodedDictionary();
        if (isDictionaryTooLarge(encodedDictionary)) {
          return bestEncoding;
        }
        int dictionaryLength = estimateDictionaryIndicesColumnLength(plainStats) + encodedDictionary.getLength();
        if (dictionaryLength < bestLength) {
          bestLength = dictionaryLength;
          bestEncoding = Types.DICTIONARY;
        }
        int frequencyLength = estimateFrequencyIndicesColumnLength(plainStats) + encodedDictionary.getLength();
        if (frequencyLength < bestLength) {
          bestLength = frequencyLength;
          bestEncoding = Types.FREQUENCY;
        }
      }

      return bestEncoding;
    }
  }

}
