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

import clojure.lang.IFn;
import clojure.lang.IPersistentMap;
import clojure.lang.IPersistentCollection;
import clojure.lang.ITransientCollection;
import clojure.lang.ISeq;
import clojure.lang.RT;

import java.nio.ByteBuffer;

public final class DataPage {

  public final static class Header implements IPageHeader, IWriteable {

    private final int numValues;
    private final int repetitionLevelsLength;
    private final int definitionLevelsLength;
    private final int compressedDataLength;
    private final int uncompressedDataLength;

    Header(int numValues, int repetitionLevelsLength, int definitionLevelsLength,
           int compressedDataLength, int uncompressedDataLength) {
      this.numValues = numValues;
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
      return Stats.dataPageStats(numValues, headerLength() + bodyLength(), headerLength(),
                                 repetitionLevelsLength, definitionLevelsLength, compressedDataLength);
    }

    @Override
    public void writeTo(MemoryOutputStream mos) {
      Bytes.writeUInt(mos, numValues);
      Bytes.writeUInt(mos, repetitionLevelsLength);
      Bytes.writeUInt(mos, definitionLevelsLength);
      Bytes.writeUInt(mos, compressedDataLength);
      Bytes.writeUInt(mos, uncompressedDataLength);
    }

    static Header read(ByteBuffer bb) {
      return new Header(Bytes.readUInt(bb), Bytes.readUInt(bb), Bytes.readUInt(bb),
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

    Writer(IEncoder repetitionLevelEncoder, IEncoder definitionLevelEncoder, IEncoder dataEncoder,
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
                        (repetitionLevelEncoder != null)? repetitionLevelEncoder.estimatedLength() : 0,
                        (definitionLevelEncoder != null)? definitionLevelEncoder.estimatedLength() : 0,
                        (int)(estimatedDataLength * getCompressionRatio()),
                        estimatedDataLength);
    }

    @Override
    public Header header() {
      int length = dataEncoder.length();
      return new Header(numValues,
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

  private final static class RequiredValuesWriter extends Writer {
    RequiredValuesWriter(IEncoder dataEncoder, ICompressor compressor) {
      super(null, null, dataEncoder, compressor);
    }

    @Override
    public void write(IPersistentCollection values) {
      for (ISeq s = RT.seq(values); s != null; s = s.next()) {
        dataEncoder.encode(s.first());
      }
      numValues += RT.count(values);
    }
  }

  private final static class NonRepeatedValuesWriter extends Writer {
    NonRepeatedValuesWriter(IEncoder definitionLevelEncoder, IEncoder dataEncoder, ICompressor compressor) {
      super(null, definitionLevelEncoder, dataEncoder, compressor);
    }

    @Override
    public void write(IPersistentCollection values) {
      for (ISeq s = RT.seq(values); s != null; s = s.next()) {
        Object v = s.first();
        if (v == null) {
          definitionLevelEncoder.encode(0);
        } else {
          definitionLevelEncoder.encode(1);
          dataEncoder.encode(v);
        }
      }
      numValues += RT.count(values);
    }
  }

  private final static class RepeatedValuesWriter extends Writer {
    RepeatedValuesWriter(IEncoder repetitionLevelEncoder, IEncoder definitionLevelEncoder,
                         IEncoder dataEncoder, ICompressor compressor) {
      super(repetitionLevelEncoder, definitionLevelEncoder, dataEncoder, compressor);
    }

    @Override
    public void write(IPersistentCollection leveledValues) {
      for (ISeq s = RT.seq(leveledValues); s != null; s = s.next()) {
        LeveledValue lv = (LeveledValue)s.first();
        if (lv.value != null) {
          dataEncoder.encode(lv.value);
        }
        repetitionLevelEncoder.encode(lv.repetitionLevel);
        definitionLevelEncoder.encode(lv.definitionLevel);
      }
      numValues += RT.count(leveledValues);
    }
  }

  public abstract static class Reader implements IPageReader {

    final ByteBuffer bb;
    final IDecoderFactory decoderFactory;
    final IDecompressorFactory decompressorFactory;
    final Header header;
    final int maxRepetitionLevel;
    final int maxDefinitionLevel;

    Reader(ByteBuffer bb, IDecoderFactory decoderFactory, IDecompressorFactory decompressorFactory,
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
      if (maxDefinitionLevel == 0) {
        return new RequiredValuesReader(byteBuffer, decoderFactory, decompressorFactory, header,
                                        maxRepetitionLevel, maxDefinitionLevel);
      } else if (maxRepetitionLevel == 0) {
        return new NonRepeatedValuesReader(byteBuffer, decoderFactory, decompressorFactory, header,
                                           maxRepetitionLevel, maxDefinitionLevel);
      } else {
        return new RepeatedValuesReader(byteBuffer, decoderFactory, decompressorFactory, header,
                                        maxRepetitionLevel, maxDefinitionLevel);
      }
    }

    @Override
    public ByteBuffer next() {
      return Bytes.sliceAhead(bb, header.bodyLength());
    }

    IIntDecoder getRepetitionLevelsDecoder() {
      return Types.levelsDecoder(Bytes.sliceAhead(bb, header.byteOffsetRepetitionLevels()),
                                 maxRepetitionLevel);
    }

    IIntDecoder getDefinitionLevelsDecoder() {
      return Types.levelsDecoder(Bytes.sliceAhead(bb, header.byteOffsetDefinitionLevels()),
                                 maxDefinitionLevel);
    }

    IDecoder getDataDecoder() {
      ByteBuffer byteBuffer = Bytes.sliceAhead(bb, header.byteOffsetData());
      if (decompressorFactory != null) {
        IDecompressor decompressor = decompressorFactory.create();
        byteBuffer = decompressor.decompress(byteBuffer,
                                             header.compressedDataLength(),
                                             header.uncompressedDataLength());
      }
      return decoderFactory.create(byteBuffer);
    }

    public IPersistentCollection read() {
      return read(null);
    }

    public abstract IPersistentCollection read(Object nullValue);

    public IPersistentCollection readWith(IFn fn) {
      return readWith(fn, fn.invoke(null));
    }

    public abstract IPersistentCollection readWith(IFn fn, Object nullValue);

  }

  private final static class RequiredValuesReader extends Reader {
    RequiredValuesReader(ByteBuffer bb, IDecoderFactory decoderFactory,
                         IDecompressorFactory decompressorFactory, Header header, int maxRepetitionLevel,
                         int maxDefinitionLevel) {
      super(bb, decoderFactory, decompressorFactory, header, maxRepetitionLevel, maxDefinitionLevel);
    }

    @Override
    public IPersistentCollection read(Object nullValue) {
      IDecoder dataDecoder = getDataDecoder();
      ITransientCollection vs = ChunkedPersistentList.newEmptyTransient();
      int i = 0;
      int n = dataDecoder.numEncodedValues();
      while (i < n) {
        vs.conj(dataDecoder.decode());
        i += 1;
      }
      return vs.persistent();
    }

    @Override
    public IPersistentCollection readWith(IFn fn, Object nullValue) {
      IDecoder dataDecoder = getDataDecoder();
      ITransientCollection vs = ChunkedPersistentList.newEmptyTransient();
      int i = 0;
      int n = dataDecoder.numEncodedValues();
      while (i < n) {
        vs.conj(fn.invoke(dataDecoder.decode()));
        i += 1;
      }
      return vs.persistent();
    }

  }

  private final static class NonRepeatedValuesReader extends Reader {
    NonRepeatedValuesReader(ByteBuffer bb, IDecoderFactory decoderFactory,
                            IDecompressorFactory decompressorFactory, Header header, int maxRepetitionLevel,
                            int maxDefinitionLevel) {
      super(bb, decoderFactory, decompressorFactory, header, maxRepetitionLevel, maxDefinitionLevel);
    }

    @Override
    public IPersistentCollection read(Object nullValue) {
      IDecoder dataDecoder = getDataDecoder();
      IIntDecoder definitionLevelsDecoder = getDefinitionLevelsDecoder();
      ITransientCollection vs = ChunkedPersistentList.newEmptyTransient();
      int i = 0;
      int n = definitionLevelsDecoder.numEncodedValues();
      while (i < n) {
        if (definitionLevelsDecoder.decodeInt() == 0) {
          vs.conj(nullValue);
        } else {
          vs.conj(dataDecoder.decode());
        }
        i += 1;
      }
      return vs.persistent();
    }

    @Override
    public IPersistentCollection readWith(IFn fn, Object nullValue) {
      IDecoder dataDecoder = getDataDecoder();
      IIntDecoder definitionLevelsDecoder = getDefinitionLevelsDecoder();
      ITransientCollection vs = ChunkedPersistentList.newEmptyTransient();
      int i = 0;
      int n = definitionLevelsDecoder.numEncodedValues();
      while (i < n) {
        if (definitionLevelsDecoder.decodeInt() == 0) {
          vs.conj(nullValue);
        } else {
          vs.conj(fn.invoke(dataDecoder.decode()));
        }
        i += 1;
      }
      return vs.persistent();
    }

  }


  private final static class RepeatedValuesReader extends Reader {
    RepeatedValuesReader(ByteBuffer bb, IDecoderFactory decoderFactory,
                         IDecompressorFactory decompressorFactory, Header header, int maxRepetitionLevel,
                         int maxDefinitionLevel) {
      super(bb, decoderFactory, decompressorFactory, header, maxRepetitionLevel, maxDefinitionLevel);
    }

    @Override
    public IPersistentCollection read(Object nullValue) {
      IDecoder dataDecoder = getDataDecoder();
      IIntDecoder repetitionLevelsDecoder = getRepetitionLevelsDecoder();
      IIntDecoder definitionLevelsDecoder = getDefinitionLevelsDecoder();
      ITransientCollection vs = ChunkedPersistentList.newEmptyTransient();
      ITransientCollection rv = ChunkedPersistentList.newEmptyTransient();
      boolean seenFirstValue = false;
      int i = 0;
      int n = repetitionLevelsDecoder.numEncodedValues();
      while (i < n) {
        int repLvl = repetitionLevelsDecoder.decodeInt();
        if (repLvl == 0 && seenFirstValue){
          vs.conj(rv.persistent());
          rv = ChunkedPersistentList.newEmptyTransient();
        }
        seenFirstValue = true;
        int defLvl = definitionLevelsDecoder.decodeInt();
        if (defLvl < maxDefinitionLevel) {
          rv.conj(new LeveledValue(repLvl, defLvl, nullValue));
        } else {
          rv.conj(new LeveledValue(repLvl, defLvl, dataDecoder.decode()));
        }
        i += 1;
      }
      if (rv != null) {
        vs.conj(rv.persistent());
      }
      return vs.persistent();
    }

    @Override
    public IPersistentCollection readWith(IFn fn, Object nullValue) {
      IDecoder dataDecoder = getDataDecoder();
      IIntDecoder repetitionLevelsDecoder = getRepetitionLevelsDecoder();
      IIntDecoder definitionLevelsDecoder = getDefinitionLevelsDecoder();
      ITransientCollection vs = ChunkedPersistentList.newEmptyTransient();
      ITransientCollection rv = ChunkedPersistentList.newEmptyTransient();
      boolean seenFirstValue = false;
      int i = 0;
      int n = repetitionLevelsDecoder.numEncodedValues();
      while (i < n) {
        int repLvl = repetitionLevelsDecoder.decodeInt();
        if (repLvl == 0 && seenFirstValue){
          vs.conj(rv.persistent());
          rv = ChunkedPersistentList.newEmptyTransient();
        }
        seenFirstValue = true;
        int defLvl = definitionLevelsDecoder.decodeInt();
        if (defLvl < maxDefinitionLevel) {
          rv.conj(new LeveledValue(repLvl, defLvl, nullValue));
        } else {
          rv.conj(new LeveledValue(repLvl, defLvl, fn.invoke(dataDecoder.decode())));
        }
        i += 1;
      }
      if (rv != null) {
        vs.conj(rv.persistent());
      }
      return vs.persistent();
    }
  }

}
