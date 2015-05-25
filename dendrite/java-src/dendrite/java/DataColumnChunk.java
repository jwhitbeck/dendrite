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

import clojure.lang.IPersistentCollection;
import clojure.lang.IPersistentMap;
import clojure.lang.ISeq;
import clojure.lang.RT;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

public final class DataColumnChunk {

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
      return Pages.readAndPartitionDataPages(Bytes.sliceAhead(bb, columnChunkMetadata.dataPageOffset),
                                             columnChunkMetadata.numDataPages,
                                             partitionLength,
                                             column.repetitionLevel,
                                             column.definitionLevel,
                                             types.getDecoderFactory(column.type, column.encoding, column.fn),
                                             types.getDecompressorFactory(column.compression)).iterator();
    }

    @Override
    public Iterable<IPageHeader> getPageHeaders() {
      return Pages.getHeaders(Bytes.sliceAhead(bb, columnChunkMetadata.dataPageOffset),
                              columnChunkMetadata.numDataPages);
    }

    public Iterable<DataPage.Reader> getPageReaders() {
      return Pages.getDataPageReaders(Bytes.sliceAhead(bb, columnChunkMetadata.dataPageOffset),
                                      columnChunkMetadata.numDataPages,
                                      column.repetitionLevel,
                                      column.definitionLevel,
                                      types.getDecoderFactory(column.type, column.encoding, column.fn),
                                      types.getDecompressorFactory(column.compression));
    }

    @Override
    public IPersistentMap stats() {
      return Stats.columnChunkStats(Pages.getPagesStats(RT.seq(getPageHeaders())));
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

  public abstract static class Writer implements IColumnChunkWriter {

    int nextNumValuesForPageLengthCheck;
    int numPages;
    final int targetDataPageLength;
    final Schema.Column column;
    final MemoryOutputStream mos;
    final DataPage.Writer pageWriter;

    Writer(DataPage.Writer pageWriter, Schema.Column column, int targetDataPageLength) {
      this.mos = new MemoryOutputStream();
      this.pageWriter = pageWriter;
      this.column = column;
      this.targetDataPageLength = targetDataPageLength;
      this.numPages = 0;
      this.nextNumValuesForPageLengthCheck = 100;
    }

    public static Writer create(Types types, Schema.Column column, int targetDataPageLength) {
      DataPage.Writer pageWriter
        = DataPage.Writer.create(column.repetitionLevel,
                                 column.definitionLevel,
                                 types.getEncoder(column.type, column.encoding),
                                 types.getCompressor(column.compression));
      return create(pageWriter, column, targetDataPageLength);
    }

    public static Writer create(DataPage.Writer pageWriter, Schema.Column column, int targetDataPageLength) {
      if (column.repetitionLevel == 0) {
        return new NonRepeatedWriter(pageWriter, column, targetDataPageLength);
      } else {
        return new RepeatedWriter(pageWriter, column, targetDataPageLength);
      }
    }

    @Override
    public abstract void write(Iterable<Object> values);

    @Override
    public Schema.Column column() {
      return column;
    }

    @Override
    public ByteBuffer byteBuffer() {
      finish();
      return mos.byteBuffer();
    }

    @Override
    public Metadata.ColumnChunk metadata() {
      finish();
      return new Metadata.ColumnChunk(length(), numPages, 0, 0);
    }

    @Override
    public int numDataPages() {
      return numPages;
    }

    @Override
    public void finish() {
      flushDataPageWriter();
    }

    @Override
    public void reset() {
      numPages = 0;
      mos.reset();
      pageWriter.reset();
    }

    @Override
    public int length() {
      finish();
      return mos.length();
    }

    @Override
    public int estimatedLength() {
      return mos.length() + pageWriter.estimatedLength();
    }

    @Override
    public void writeTo(MemoryOutputStream memoryOutputStream) {
      finish();
      memoryOutputStream.write(mos);
    }

    void flushDataPageWriter() {
      if (pageWriter.numValues() > 0) {
        Pages.writeTo(mos, pageWriter);
        numPages += 1;
        nextNumValuesForPageLengthCheck = pageWriter.numValues() / 2;
        pageWriter.reset();
      }
    }
  }

  private static final class NonRepeatedWriter extends Writer {
    NonRepeatedWriter(DataPage.Writer pageWriter, Schema.Column column, int targetDataPageLength) {
      super(pageWriter, column, targetDataPageLength);
    }

    @Override
    public void write(Iterable<Object> values) {
      Iterator<Object> vi = values.iterator();
      while (vi.hasNext()) {
        if (pageWriter.numValues() >= targetDataPageLength) {
          // Some pages compress "infinitely" well (e.g., a run-length encoded list of zeros). Since pages are
          // fully realized when read, this can lead to memory issues when deserializng so we cap the total
          // number of values in a page here to one value per byte.
          flushDataPageWriter();
        }
        int numValuesBeforeNextCheck = nextNumValuesForPageLengthCheck - pageWriter.numValues();
        while (numValuesBeforeNextCheck > 0 && vi.hasNext()) {
          pageWriter.write(vi.next());
          numValuesBeforeNextCheck -= 1;
        }
        if (vi.hasNext() && numValuesBeforeNextCheck == 0) {
          if (pageWriter.estimatedLength() > targetDataPageLength) {
            flushDataPageWriter();
          } else {
            nextNumValuesForPageLengthCheck = Thresholds.nextCheckThreshold(pageWriter.numValues(),
                                                                            pageWriter.estimatedLength(),
                                                                            targetDataPageLength);
          }
        }
      }
    }
  }

  private static final class RepeatedWriter extends Writer {
    RepeatedWriter(DataPage.Writer pageWriter, Schema.Column column, int targetDataPageLength) {
      super(pageWriter, column, targetDataPageLength);
    }

    @Override
    public void write(Iterable<Object> leveledValues) {
      Iterator<Object> lvi = leveledValues.iterator();
      while (lvi.hasNext()) {
        if (pageWriter.numValues() >= targetDataPageLength) {
          // Some pages compress "infinitely" well (e.g., a run-length encoded list of zeros). Since pages are
          // fully realized when read, this can lead to memory issues when deserializng so we cap the total
          // number of values in a page here to one value per byte.
          flushDataPageWriter();
        }
        while (pageWriter.numValues() <= nextNumValuesForPageLengthCheck && lvi.hasNext()) {
          pageWriter.write(lvi.next());
        }
        if (pageWriter.numValues() > nextNumValuesForPageLengthCheck) {
          if (pageWriter.estimatedLength() > targetDataPageLength) {
            flushDataPageWriter();
          } else {
            nextNumValuesForPageLengthCheck = Thresholds.nextCheckThreshold(pageWriter.numValues(),
                                                                            pageWriter.estimatedLength(),
                                                                            targetDataPageLength);
          }
        }
      }
    }
  }
}
