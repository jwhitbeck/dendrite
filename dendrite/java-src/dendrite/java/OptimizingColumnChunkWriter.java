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
import clojure.lang.IMapEntry;
import clojure.lang.IPersistentMap;
import clojure.lang.Symbol;
import clojure.lang.RT;

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

  abstract int getBestEncoding(DataColumnChunk.Reader plainReader, IPersistentMap plainStats);

  private static final int PARTITION_LENGTH = 100;

  public IColumnChunkWriter optimize(IPersistentMap compressionThresholds) {
    plainColumnChunkWriter.finish();
    DataColumnChunk.Reader primitiveReader
      = new DataColumnChunk.Reader(types, plainColumnChunkWriter.byteBuffer(),
                                   plainColumnChunkWriter.metadata(),
                                   primitiveColumn, PARTITION_LENGTH);
    IPersistentMap plainStats = primitiveReader.stats();
    int bestEncoding = getBestEncoding(primitiveReader, plainStats);
    int bestCompression = getBestCompression(bestEncoding, primitiveReader, plainStats, compressionThresholds);
    DataColumnChunk.Reader plainReader
      = new DataColumnChunk.Reader(types, plainColumnChunkWriter.byteBuffer(),
                                   plainColumnChunkWriter.metadata(),
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
                         IPersistentMap plainStats, IPersistentMap compressionThresholds) {
    if (bestEncoding == Types.DICTIONARY || bestEncoding == Types.FREQUENCY) {
      return getDictionaryBestCompression(bestEncoding, plainStats, compressionThresholds);
    } else {
      return getRegularBestCompression(bestEncoding, primitiveReader, plainStats, compressionThresholds);
    }
  }

  private int getRegularBestCompression(int encoding, DataColumnChunk.Reader primitiveReader,
                                IPersistentMap plainStats, IPersistentMap compressionThresholds) {
    IColumnChunkWriter writer
      = DataColumnChunk.Writer.create(types,
                                      primitiveColumn.withEncoding(encoding),
                                      plainColumnChunkWriter.targetDataPageLength);
    copyAtLeastOnePage(primitiveReader, writer);
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
    int noCompressionLength = levelsLength + (int)(h.uncompressedDataLength() * nonNilValuesMultiplier);
    int bestLength = noCompressionLength;
    for (Object o : compressionThresholds.without(Types.NONE_SYM)) {
      IMapEntry e = (IMapEntry)o;
      int compression = types.getCompression((Symbol)e.key());
      double threshold = RT.doubleCast(e.val());
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

  private void copyAtLeastOnePage(DataColumnChunk.Reader primitiveReader,
                                  IColumnChunkWriter columnChunkWriter) {
    Iterator<DataPage.Reader> i = primitiveReader.getPageReaders().iterator();
    while (i.hasNext() && columnChunkWriter.numDataPages() == 0) {
      columnChunkWriter.write(i.next());
    }
  }

  private int getDictionaryBestCompression(int encoding, IPersistentMap plainStats,
                                           IPersistentMap compressionThresholds) {
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
    int noCompressionLength = indicesColumnLength + primitiveEncoder.length();
    int bestLength = noCompressionLength;
    for (Object o : compressionThresholds.without(Types.NONE_SYM)) {
      IMapEntry e = (IMapEntry)o;
      int compression = types.getCompression((Symbol)e.key());
      double threshold = (double)e.val();
      ICompressor compressor = types.getCompressor(compression);
      compressor.compress(primitiveEncoder);
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

  private int estimateLevelsLength(IPersistentMap plainStats) {
    return (int)RT.get(plainStats, Stats.REPETITION_LEVELS_LENGTH)
      + (int)RT.get(plainStats, Stats.DEFINITION_LEVELS_LENGTH);
  }

  int estimateDictionaryIndicesColumnLength(IPersistentMap plainStats) {
    int numEntries = statsCollector.getFrequencies().size();
    int width = Bytes.getBitWidth(numEntries);
    int numNonNilValues = (int)plainStats.valAt(Stats.NUM_NON_NIL_VALUES);
    return (numNonNilValues * width) / 8;
  }

  int estimateFrequencyIndicesColumnLength(IPersistentMap plainStats) {
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
  public Metadata.ColumnChunk metadata() {
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
    int getBestEncoding(DataColumnChunk.Reader primitiveReader, IPersistentMap plainStats) {
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
    int unCompressedLength = encodedDictionary.length();
    if (unCompressedLength < targetDataPageLength) {
      return false;
    } else {
      ICompressor compressor = types.getCompressor(Types.DEFLATE);
      compressor.compress(encodedDictionary);
      compressor.finish();
      return compressor.length() > targetDataPageLength;
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
    IPersistentMap pageStats = pageWriter.header().stats();
    int numNonNilValuesInPage = (int)RT.get(pageStats, Stats.NUM_NON_NIL_VALUES);
    double mult = ((double)numNonNilValues/(double)numNonNilValuesInPage);
    return (int)((int)RT.get(pageStats, Stats.DATA_LENGTH) * mult);
  }

  private static final class DefaultColumnChunk extends OptimizingColumnChunkWriter {

    private DefaultColumnChunk(Types types, DataColumnChunk.Writer plainColumnChunkWriter,
                               Schema.Column column, StatsCollector statsCollector) {
      super(types, plainColumnChunkWriter, column, statsCollector);
    }

    @Override
    int getBestEncoding(DataColumnChunk.Reader primitiveReader, IPersistentMap plainStats) {
      if (statsCollector.isDictionarySaturated()) {
        return Types.PLAIN;
      }
      IOutputBuffer encodedDictionary = getEncodedDictionary();
      if (isDictionaryTooLarge(encodedDictionary)) {
        return Types.PLAIN;
      }
      int plainLength = (int)RT.get(plainStats, Stats.DATA_LENGTH);
      int bestLength = plainLength;
      int bestEncoding = Types.PLAIN;
      int dictionaryLength = estimateDictionaryIndicesColumnLength(plainStats) + encodedDictionary.length();
      if (dictionaryLength < bestLength) {
        bestLength = dictionaryLength;
        bestEncoding = Types.DICTIONARY;
      }
      int frequencyLength = estimateFrequencyIndicesColumnLength(plainStats) + encodedDictionary.length();
      if (frequencyLength < bestLength) {
        bestLength = frequencyLength;
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
    int getBestEncoding(DataColumnChunk.Reader primitiveReader, IPersistentMap plainStats) {
      int plainLength = (int)RT.get(plainStats, Stats.DATA_LENGTH);
      int bestLength = plainLength;
      int bestEncoding = Types.PLAIN;
      int numNonNilValues = (int)plainStats.valAt(Stats.NUM_NON_NIL_VALUES);

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
            + encodedDictionary.length();
          if (dictionaryLength < bestLength) {
            bestLength = dictionaryLength;
            bestEncoding = Types.DICTIONARY;
          }
          int frequencyLength = estimateFrequencyIndicesColumnLength(plainStats)
            + encodedDictionary.length();
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
    int getBestEncoding(DataColumnChunk.Reader primitiveReader, IPersistentMap plainStats) {
      int plainLength = (int)RT.get(plainStats, Stats.DATA_LENGTH);
      int bestLength = plainLength;
      int bestEncoding = Types.PLAIN;
      int numNonNilValues = (int)plainStats.valAt(Stats.NUM_NON_NIL_VALUES);

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
            + encodedDictionary.length();
          if (dictionaryLength < bestLength) {
            bestLength = dictionaryLength;
            bestEncoding = Types.DICTIONARY;
          }
          int frequencyLength = estimateFrequencyIndicesColumnLength(plainStats)
            + encodedDictionary.length();
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
    int getBestEncoding(DataColumnChunk.Reader primitiveReader, IPersistentMap plainStats) {
      int plainLength = (int)RT.get(plainStats, Stats.DATA_LENGTH);
      int bestLength = plainLength;
      int bestEncoding = Types.DELTA_LENGTH;
      int numNonNilValues = (int)plainStats.valAt(Stats.NUM_NON_NIL_VALUES);

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
        int dictionaryLength = estimateDictionaryIndicesColumnLength(plainStats) + encodedDictionary.length();
        if (dictionaryLength < bestLength) {
          bestLength = dictionaryLength;
          bestEncoding = Types.DICTIONARY;
        }
        int frequencyLength = estimateFrequencyIndicesColumnLength(plainStats) + encodedDictionary.length();
        if (frequencyLength < bestLength) {
          bestLength = frequencyLength;
          bestEncoding = Types.FREQUENCY;
        }
      }

      return bestEncoding;
    }
  }

}
