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

public final class DictionaryPage {

  public static final class Header implements IPageHeader, IWriteable {

    private final int numValues;
    private final int compressedDataLength;
    private final int uncompressedDataLength;

    private Header(int numValues, int compressedDataLength, int uncompressedDataLength) {
      this.numValues = numValues;
      this.compressedDataLength = compressedDataLength;
      this.uncompressedDataLength = uncompressedDataLength;
    }

    @Override
    public int getType() {
      return Pages.DICTIONARY;
    }

    @Override
    public int getHeaderLength() {
      return Bytes.getNumUIntBytes(numValues)
        + Bytes.getNumUIntBytes(compressedDataLength) + Bytes.getNumUIntBytes(uncompressedDataLength);
    }

    @Override
    public int getBodyLength() {
      return compressedDataLength;
    }

    public int getByteOffsetData() {
      return 0;
    }

    public int getCompressedDataLength() {
      return compressedDataLength;
    }

    public int getUncompressedDataLength() {
      return uncompressedDataLength;
    }

    @Override
    public Stats.Page getStats() {
      return Stats.createDictionaryPageStats(numValues, getHeaderLength() + getBodyLength(), getHeaderLength(),
                                             compressedDataLength);
    }

    @Override
    public void writeTo(MemoryOutputStream mos) {
      Bytes.writeUInt(mos, numValues);
      Bytes.writeUInt(mos, compressedDataLength);
      Bytes.writeUInt(mos, uncompressedDataLength);
    }

    public static Header read(ByteBuffer bb) {
      return new Header(Bytes.readUInt(bb), Bytes.readUInt(bb), Bytes.readUInt(bb));
    }

  }

  public static final class Writer implements IPageWriter {

    final IEncoder dataEncoder;
    final ICompressor compressor;
    boolean isFinished = false;

    private Writer(IEncoder dataEncoder, ICompressor compressor) {
      this.dataEncoder = dataEncoder;
      this.compressor = compressor;
    }

    public static Writer create(IEncoder dataEncoder, ICompressor compressor) {
      return new Writer(dataEncoder, compressor);
    }

    @Override
    public int getNumValues() {
      return dataEncoder.getNumEncodedValues();
    }

    @Override
    public void write(Object value) {
      dataEncoder.encode(value);
    }

    @Override
    public Header getHeader() {
      int length = dataEncoder.getLength();
      return new Header(getNumValues(), (compressor != null)? compressor.getLength() : length, length);
    }

    @Override
    public void reset() {
      isFinished = false;
      dataEncoder.reset();
      if (compressor != null) {
        compressor.reset();
      }
    }

    @Override
    public void finish() {
      if (!isFinished) {
        dataEncoder.finish();
        if (compressor != null) {
          compressor.compress(dataEncoder);
        }
        isFinished = true;
      }
    }

    @Override
    public int getLength() {
      Header h = getHeader();
      return h.getHeaderLength() + h.getBodyLength();
    }

    @Override
    public int getEstimatedLength() {
      return getLength();
    }

    @Override
    public void writeTo(MemoryOutputStream mos) {
      finish();
      mos.write(getHeader());
      if (compressor != null) {
        mos.write(compressor);
      } else {
        mos.write(dataEncoder);
      }
    }

  }

  public static final class Reader implements IPageReader {

    private final ByteBuffer bb;
    private final IDecoderFactory decoderFactory;
    private final IDecompressorFactory decompressorFactory;
    private final Header header;

    private Reader(ByteBuffer bb, IDecoderFactory decoderFactory, IDecompressorFactory decompressorFactory,
                   Header header) {
      this.bb = bb;
      this.decoderFactory = decoderFactory;
      this.decompressorFactory = decompressorFactory;
      this.header = header;
    }

    public static Reader create(ByteBuffer bb, IDecoderFactory decoderFactory,
                                IDecompressorFactory decompressorFactory) {
      ByteBuffer byteBuffer = bb.slice();
      Header header = Header.read(byteBuffer);
      return new Reader(byteBuffer, decoderFactory, decompressorFactory, header);
    }

    @Override
    public ByteBuffer getNextBuffer() {
      return Bytes.sliceAhead(bb, header.getBodyLength());
    }

    @Override
    public Header getHeader() {
      return header;
    }

    private IDecoder getDecoder() {
      ByteBuffer byteBuffer = Bytes.sliceAhead(bb, header.getByteOffsetData());
      if (decompressorFactory != null) {
        IDecompressor decompressor = decompressorFactory.create();
        byteBuffer = decompressor.decompress(byteBuffer,
                                             header.getCompressedDataLength(),
                                             header.getUncompressedDataLength());
      }
      return decoderFactory.create(byteBuffer);
    }

    public Object[] read() {
      IDecoder decoder = getDecoder();
      Object[] a = new Object[decoder.getNumEncodedValues()];
      int i = 0;
      while (i < a.length) {
        a[i] = decoder.decode();
        i += 1;
      }
      return a;
    }

  }
}
