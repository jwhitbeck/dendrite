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
    public int type() { return Pages.DICTIONARY; }

    @Override
    public int headerLength() {
      return Bytes.getNumUIntBytes(numValues)
        + Bytes.getNumUIntBytes(compressedDataLength) + Bytes.getNumUIntBytes(uncompressedDataLength);
    }

    @Override
    public int bodyLength() {
      return compressedDataLength;
    }

    public int byteOffsetData() {
      return 0;
    }

    public int compressedDataLength() {
      return compressedDataLength;
    }

    public int uncompressedDataLength() {
      return uncompressedDataLength;
    }

    @Override
    public Stats.Page stats() {
      return Stats.createDictionaryPageStats(numValues, headerLength() + bodyLength(), headerLength(),
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
    public int numValues() {
      return dataEncoder.numEncodedValues();
    }

    @Override
    public void write(Object value) {
      dataEncoder.encode(value);
    }

    @Override
    public Header header() {
      int length = dataEncoder.length();
      return new Header(numValues(), (compressor != null)? compressor.length() : length, length);
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
    public int length() {
      Header h = header();
      return h.headerLength() + h.bodyLength();
    }

    @Override
    public int estimatedLength() {
      return length();
    }

    @Override
    public void writeTo(MemoryOutputStream mos) {
      finish();
      mos.write(header());
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
    public ByteBuffer next() {
      return Bytes.sliceAhead(bb, header.bodyLength());
    }

    @Override
    public Header header() {
      return header;
    }

    private IDecoder getDecoder() {
      ByteBuffer byteBuffer = Bytes.sliceAhead(bb, header.byteOffsetData());
      if (decompressorFactory != null) {
        IDecompressor decompressor = decompressorFactory.create();
        byteBuffer = decompressor.decompress(byteBuffer,
                                             header.compressedDataLength(),
                                             header.uncompressedDataLength());
      }
      return decoderFactory.create(byteBuffer);
    }

    public Object[] read() {
      IDecoder decoder = getDecoder();
      Object[] a = new Object[decoder.numEncodedValues()];
      int i = 0;
      while (i < a.length) {
        a[i] = decoder.decode();
        i += 1;
      }
      return a;
    }

  }
}
