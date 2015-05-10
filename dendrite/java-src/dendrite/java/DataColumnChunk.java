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
import clojure.lang.ISeq;
import clojure.lang.RT;

import java.nio.ByteBuffer;

public final class DataColumnChunk {

  public final static class Reader implements IColumnChunkReader {

    private final ByteBuffer bb;
    private final ColumnChunkMetadata columnChunkMetadata;
    private final Types types;
    private final Schema.Column column;

    public Reader(Types types, ByteBuffer bb, ColumnChunkMetadata columnChunkMetadata,
                  Schema.Column column) {
      this.types = types;
      this.bb = bb;
      this.columnChunkMetadata = columnChunkMetadata;
      this.column = column;
    }

    @Override
    public ISeq readPartitioned(int partitionLength) {
      return Pages.readDataPagesPartitioned(Bytes.sliceAhead(bb, columnChunkMetadata.dataPageOffset),
                                            columnChunkMetadata.numDataPages,
                                            partitionLength,
                                            column.repetitionLevel,
                                            column.definitionLevel,
                                            types.getDecoderFactory(column.type, column.encoding),
                                            types.getDecompressorFactory(column.compression),
                                            column.fn);
    }

    @Override
    public ISeq getPageHeaders() {
      return Pages.readHeaders(Bytes.sliceAhead(bb, columnChunkMetadata.dataPageOffset),
                               columnChunkMetadata.numDataPages);
    }

    @Override
    public ColumnChunkMetadata metadata() {
      return columnChunkMetadata;
    }

  }

  public static abstract class Writer implements IColumnChunkWriter {
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
    public abstract void write(ChunkedPersistentList values);

    @Override
    public Schema.Column column() {
      return column;
    }

    @Override
    public ColumnChunkMetadata metadata() {
      return new ColumnChunkMetadata(length(), numPages, 0, 0);
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
        nextNumValuesForPageLengthCheck >>>= 1;
        pageWriter.reset();
      }
    }
  }

  private final static class NonRepeatedWriter extends Writer {
    NonRepeatedWriter(DataPage.Writer pageWriter, Schema.Column column, int targetDataPageLength) {
      super(pageWriter, column, targetDataPageLength);
    }

    @Override
    public void write(ChunkedPersistentList values) {
      if (pageWriter.numValues() >= targetDataPageLength) {
        // Some pages compress "infinitely" well (e.g., a run-length encoded list of zeros). Since pages are
        // fully realized when read, this can lead to memory issues when deserializng so we cap the total
        // number of values in a page here to one value per byte.
        flushDataPageWriter();
      }
      int numValuesBeforeNextCheck = nextNumValuesForPageLengthCheck - pageWriter.numValues();
      if (numValuesBeforeNextCheck >= RT.count(values)) {
        pageWriter.write(values);
      } else {
        ChunkedPersistentList currentBatch = values.take(numValuesBeforeNextCheck);
        ChunkedPersistentList nextBatch = values.drop(numValuesBeforeNextCheck);
        pageWriter.write(currentBatch);
        if (pageWriter.estimatedLength() > targetDataPageLength) {
          flushDataPageWriter();
        } else {
          nextNumValuesForPageLengthCheck = Thresholds.nextCheckThreshold(pageWriter.numValues(),
                                                                          pageWriter.estimatedLength(),
                                                                          targetDataPageLength);
        }
        write(nextBatch);
      }
    }
  }

  private final static class RepeatedWriter extends Writer {
    RepeatedWriter(DataPage.Writer pageWriter, Schema.Column column, int targetDataPageLength) {
      super(pageWriter, column, targetDataPageLength);
    }

    @Override
    public void write(ChunkedPersistentList leveledValuesSeq) {
      if (pageWriter.numValues() >= targetDataPageLength) {
        // Some pages compress "infinitely" well (e.g., a run-length encoded list of zeros). Since pages are
        // fully realized when read, this can lead to memory issues when deserializng so we cap the total
        // number of values in a page here to one value per byte.
        flushDataPageWriter();
      }
      int numValuesBeforeNextCheck = nextNumValuesForPageLengthCheck - pageWriter.numValues();
      int numWrittenValues = 0;
      ChunkedPersistentList remaining = leveledValuesSeq;
      while (numWrittenValues <= numValuesBeforeNextCheck && remaining != null) {
        IPersistentCollection leveledValues = (IPersistentCollection)remaining.first();
        pageWriter.write(leveledValues);
        remaining = remaining.next();
        numWrittenValues += RT.count(leveledValues);
      }
      if (numWrittenValues > numValuesBeforeNextCheck) {
        if (pageWriter.estimatedLength() > targetDataPageLength) {
          flushDataPageWriter();
        } else {
          nextNumValuesForPageLengthCheck = Thresholds.nextCheckThreshold(pageWriter.numValues(),
                                                                          pageWriter.estimatedLength(),
                                                                          targetDataPageLength);
        }
        write(remaining);
      }
    }
  }
}
