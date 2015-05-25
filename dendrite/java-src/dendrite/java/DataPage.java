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

import clojure.lang.IPersistentMap;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public final class DataPage {

  public final static class Header implements IPageHeader, IWriteable {

    private final int numValues;
    private final int numNonNilValues;
    private final int repetitionLevelsLength;
    private final int definitionLevelsLength;
    private final int compressedDataLength;
    private final int uncompressedDataLength;

    Header(int numValues, int numNonNilValues, int repetitionLevelsLength, int definitionLevelsLength,
           int compressedDataLength, int uncompressedDataLength) {
      this.numValues = numValues;
      this.numNonNilValues = numNonNilValues;
      this.repetitionLevelsLength = repetitionLevelsLength;
      this.definitionLevelsLength = definitionLevelsLength;
      this.compressedDataLength = compressedDataLength;
      this.uncompressedDataLength = uncompressedDataLength;
    }

    @Override
    public int type() { return Pages.DATA; }

    @Override
    public int headerLength() {
      return Bytes.getNumUIntBytes(numValues)
        + Bytes.getNumUIntBytes(repetitionLevelsLength) + Bytes.getNumUIntBytes(definitionLevelsLength)
        + Bytes.getNumUIntBytes(compressedDataLength) + Bytes.getNumUIntBytes(uncompressedDataLength);
    }

    @Override
    public int bodyLength() {
      return repetitionLevelsLength + definitionLevelsLength + compressedDataLength;
    }

    public int byteOffsetData() {
      return repetitionLevelsLength + definitionLevelsLength;
    }

    public boolean hasRepetitionLevels() {
      return repetitionLevelsLength > 0;
    }

    public boolean hasDefinitionLevels() {
      return definitionLevelsLength > 0;
    }

    public int byteOffsetRepetitionLevels() {
      return 0;
    }

    public int byteOffsetDefinitionLevels() {
      return repetitionLevelsLength;
    }

    public int compressedDataLength() {
      return compressedDataLength;
    }

    public int uncompressedDataLength() {
      return uncompressedDataLength;
    }

    @Override
    public IPersistentMap stats() {
      return Stats.dataPageStats(numValues, numNonNilValues, headerLength() + bodyLength(), headerLength(),
                                 repetitionLevelsLength, definitionLevelsLength, compressedDataLength);
    }

    @Override
    public void writeTo(MemoryOutputStream mos) {
      Bytes.writeUInt(mos, numValues);
      Bytes.writeUInt(mos, numNonNilValues);
      Bytes.writeUInt(mos, repetitionLevelsLength);
      Bytes.writeUInt(mos, definitionLevelsLength);
      Bytes.writeUInt(mos, compressedDataLength);
      Bytes.writeUInt(mos, uncompressedDataLength);
    }

    static Header read(ByteBuffer bb) {
      return new Header(Bytes.readUInt(bb), Bytes.readUInt(bb), Bytes.readUInt(bb), Bytes.readUInt(bb),
                        Bytes.readUInt(bb), Bytes.readUInt(bb));

    }
  }

  public abstract static class Writer implements IPageWriter {

    final IEncoder repetitionLevelEncoder;
    final IEncoder definitionLevelEncoder;
    final IEncoder dataEncoder;
    final ICompressor compressor;
    double compressionRatio = 0;
    boolean isFinished = false;
    int numValues = 0;

    private Writer(IEncoder repetitionLevelEncoder, IEncoder definitionLevelEncoder, IEncoder dataEncoder,
                   ICompressor compressor) {
      this.repetitionLevelEncoder = repetitionLevelEncoder;
      this.definitionLevelEncoder = definitionLevelEncoder;
      this.dataEncoder = dataEncoder;
      this.compressor = compressor;
    }

    public static Writer create(int maxRepetitionLevel, int maxDefinitionLevel, IEncoder dataEncoder,
                                ICompressor compressor) {
      if (maxDefinitionLevel == 0) {
        return new RequiredValuesWriter(dataEncoder, compressor);
      } else if (maxRepetitionLevel == 0) {
        return new NonRepeatedValuesWriter(Types.levelsEncoder(maxDefinitionLevel), dataEncoder, compressor);
      } else {
        return new RepeatedValuesWriter(Types.levelsEncoder(maxRepetitionLevel),
                                        Types.levelsEncoder(maxDefinitionLevel),
                                        dataEncoder,
                                        compressor);
      }
    }

    @Override
    public int numValues() {
      return numValues;
    }

    private double getCompressionRatio() {
      if (compressor == null) {
        return 1;
      } else if (compressionRatio > 0) {
        return compressionRatio;
      } else if (dataEncoder.numEncodedValues() > 0) {
        return 0.5; // default compression ratio guess when we don't have any data.
      } else {
        return 0;
      }
    }

    private Header provisionalHeader() {
      int estimatedDataLength = dataEncoder.estimatedLength();
      return new Header(numValues,
                        dataEncoder.numEncodedValues(),
                        (repetitionLevelEncoder != null)? repetitionLevelEncoder.estimatedLength() : 0,
                        (definitionLevelEncoder != null)? definitionLevelEncoder.estimatedLength() : 0,
                        (int)(estimatedDataLength * getCompressionRatio()),
                        estimatedDataLength);
    }

    @Override
    public Header header() {
      int length = dataEncoder.length();
      return new Header(numValues,
                        dataEncoder.numEncodedValues(),
                        (repetitionLevelEncoder != null)? repetitionLevelEncoder.length() : 0,
                        (definitionLevelEncoder != null)? definitionLevelEncoder.length() : 0,
                        (compressor != null)? compressor.length() : length,
                        length);
    }

    @Override
    public void reset() {
      isFinished = false;
      numValues = 0;
      if (repetitionLevelEncoder != null) {
        repetitionLevelEncoder.reset();
      }
      if (definitionLevelEncoder != null) {
        definitionLevelEncoder.reset();
      }
      dataEncoder.reset();
      if (compressor != null) {
        compressor.reset();
      }
    }

    @Override
    public void finish() {
      if (!isFinished) {
        if (repetitionLevelEncoder != null) {
          repetitionLevelEncoder.finish();
        }
        if (definitionLevelEncoder != null) {
          definitionLevelEncoder.finish();
        }
        dataEncoder.finish();
        if (compressor != null) {
          compressor.compress(dataEncoder);
          compressionRatio = (double)compressor.length() / (double)compressor.uncompressedLength();
        }
        isFinished = true;
      }
    }

    @Override
    public int length() {
      Header h = header();
      return h.headerLength() + h.bodyLength();
    }

    @Override
    public int estimatedLength() {
      Header h = provisionalHeader();
      return h.headerLength() + h.bodyLength();
    }

    @Override
    public void writeTo(MemoryOutputStream mos) {
      finish();
      mos.write(header());
      if (repetitionLevelEncoder != null) {
        mos.write(repetitionLevelEncoder);
      }
      if (definitionLevelEncoder != null) {
        mos.write(definitionLevelEncoder);
      }
      if (compressor != null) {
        mos.write(compressor);
      } else {
        mos.write(dataEncoder);
      }
    }

  }

  private static final class RequiredValuesWriter extends Writer {
    private RequiredValuesWriter(IEncoder dataEncoder, ICompressor compressor) {
      super(null, null, dataEncoder, compressor);
    }

    @Override
    public void write(Object value) {
      dataEncoder.encode(value);
      numValues += 1;
    }
  }

  private static final class NonRepeatedValuesWriter extends Writer {
    private NonRepeatedValuesWriter(IEncoder definitionLevelEncoder, IEncoder dataEncoder,
                                    ICompressor compressor) {
      super(null, definitionLevelEncoder, dataEncoder, compressor);
    }

    @Override
    public void write(Object value) {
      if (value == null) {
        definitionLevelEncoder.encode(0);
      } else {
        definitionLevelEncoder.encode(1);
        dataEncoder.encode(value);
      }
      numValues += 1;
    }
  }

  private static final class RepeatedValuesWriter extends Writer {
    private RepeatedValuesWriter(IEncoder repetitionLevelEncoder, IEncoder definitionLevelEncoder,
                                 IEncoder dataEncoder, ICompressor compressor) {
      super(repetitionLevelEncoder, definitionLevelEncoder, dataEncoder, compressor);
    }

    @Override
    public void write(Object values) {
      for (Object o : (List)values) {
        LeveledValue lv = (LeveledValue)o;
        if (lv.value != null) {
          dataEncoder.encode(lv.value);
        }
        repetitionLevelEncoder.encode(lv.repetitionLevel);
        definitionLevelEncoder.encode(lv.definitionLevel);
        numValues += 1;
      }
    }
  }

  public static final class Reader implements IPageReader, Iterable<Object> {

    private final ByteBuffer bb;
    private final IDecoderFactory decoderFactory;
    private final IDecompressorFactory decompressorFactory;
    private final Header header;
    private final int maxRepetitionLevel;
    private final int maxDefinitionLevel;

    private Reader(ByteBuffer bb, IDecoderFactory decoderFactory, IDecompressorFactory decompressorFactory,
                   Header header, int maxRepetitionLevel, int maxDefinitionLevel) {
      this.bb = bb;
      this.decoderFactory = decoderFactory;
      this.decompressorFactory = decompressorFactory;
      this.header = header;
      this.maxRepetitionLevel = maxRepetitionLevel;
      this.maxDefinitionLevel = maxDefinitionLevel;
    }

    public static Reader create(ByteBuffer bb, int maxRepetitionLevel, int maxDefinitionLevel,
                                IDecoderFactory decoderFactory, IDecompressorFactory decompressorFactory) {
      ByteBuffer byteBuffer = bb.slice();
      Header header = Header.read(byteBuffer);
      return new Reader(byteBuffer, decoderFactory, decompressorFactory, header, maxRepetitionLevel,
                        maxDefinitionLevel);
    }

    @Override
    public ByteBuffer next() {
      return Bytes.sliceAhead(bb, header.bodyLength());
    }

    @Override
    public Header header() {
      return header;
    }

    @Override
    public Iterator<Object> iterator() {
      if (maxDefinitionLevel == 0) {
        return new RequiredValueIterator(getDataDecoder());
      } else if (maxRepetitionLevel == 0) {
        return new NonRepeatedValueIterator(getDefinitionLevelsDecoder(), getDataDecoder(),
                                            decoderFactory.nullValue());
      } else {
        return new RepeatedValueIterator(getRepetitionLevelsDecoder(), getDefinitionLevelsDecoder(),
                                         getDataDecoder(), decoderFactory.nullValue(), maxDefinitionLevel);
      }
    }

    private IIntDecoder getRepetitionLevelsDecoder() {
      return Types.levelsDecoder(Bytes.sliceAhead(bb, header.byteOffsetRepetitionLevels()),
                                 maxRepetitionLevel);
    }

    private IIntDecoder getDefinitionLevelsDecoder() {
      return Types.levelsDecoder(Bytes.sliceAhead(bb, header.byteOffsetDefinitionLevels()),
                                 maxDefinitionLevel);
    }

    private IDecoder getDataDecoder() {
      ByteBuffer byteBuffer = Bytes.sliceAhead(bb, header.byteOffsetData());
      if (decompressorFactory != null) {
        IDecompressor decompressor = decompressorFactory.create();
        byteBuffer = decompressor.decompress(byteBuffer,
                                             header.compressedDataLength(),
                                             header.uncompressedDataLength());
      }
      return decoderFactory.create(byteBuffer);
    }
  }

  private static final class RequiredValueIterator extends AReadOnlyIterator<Object> {

    private final IDecoder decoder;
    private final int n;
    private int i;

    private RequiredValueIterator(IDecoder decoder) {
      this.n = decoder.numEncodedValues();
      this.i = 0;
      this.decoder = decoder;
    }

    @Override
    public boolean hasNext() {
      return i < n;
    }

    @Override
    public Object next() {
      i += 1;
      return decoder.decode();
    }
  }

  private static final class NonRepeatedValueIterator extends AReadOnlyIterator<Object> {

    private final IDecoder decoder;
    private final IIntDecoder definitionLevelsDecoder;
    private final Object nullValue;
    private final int n;
    private int i;

    NonRepeatedValueIterator(IIntDecoder definitionLevelsDecoder, IDecoder decoder, Object nullValue) {
      this.n = definitionLevelsDecoder.numEncodedValues();
      this.i = 0;
      this.decoder = decoder;
      this.definitionLevelsDecoder = definitionLevelsDecoder;
      this.nullValue = nullValue;
    }

    @Override
    public boolean hasNext() {
      return i < n;
    }

    @Override
    public Object next() {
      i += 1;
      if (definitionLevelsDecoder.decodeInt() == 0) {
        return nullValue;
      } else {
        return decoder.decode();
      }
    }
  }

  private static final class RepeatedValueIterator extends AReadOnlyIterator<Object> {

    private final IIntDecoder repetitionLevelsDecoder;
    private final IIntDecoder definitionLevelsDecoder;
    private final int maxDefinitionLevel;
    private final IDecoder decoder;
    private final Object nullValue;
    private final int n;
    private int i;
    private int nextRepetitionLevel;
    private boolean seenFirstValue = false;

    private RepeatedValueIterator(IIntDecoder repetitionLevelsDecoder, IIntDecoder definitionLevelsDecoder,
                                  IDecoder decoder, Object nullValue, int maxDefinitionLevel) {
      this.n = repetitionLevelsDecoder.numEncodedValues();
      this.i = 0;
      this.decoder = decoder;
      this.repetitionLevelsDecoder = repetitionLevelsDecoder;
      this.definitionLevelsDecoder = definitionLevelsDecoder;
      this.nullValue = nullValue;
      this.maxDefinitionLevel = maxDefinitionLevel;
      if (n > 0) {
        this.nextRepetitionLevel = repetitionLevelsDecoder.decodeInt();
      }
    }

    @Override
    public boolean hasNext() {
      return i < n;
    }

    @Override
    public Object next() {
      List<LeveledValue> nextRepeatedValues = new ArrayList<LeveledValue>();
      while (i < n) {
        int definitionLevel = definitionLevelsDecoder.decodeInt();
        if (definitionLevel < maxDefinitionLevel) {
          nextRepeatedValues.add(new LeveledValue(nextRepetitionLevel, definitionLevel, nullValue));
        } else {
          nextRepeatedValues.add(new LeveledValue(nextRepetitionLevel, definitionLevel, decoder.decode()));
        }
        i += 1;
        if (i < n) {
          nextRepetitionLevel = repetitionLevelsDecoder.decodeInt();
          if (nextRepetitionLevel == 0) {
            break;
          }
        }
      }
      return nextRepeatedValues;
    }
  }

}
