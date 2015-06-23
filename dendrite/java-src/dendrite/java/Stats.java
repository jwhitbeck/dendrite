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

import clojure.lang.IPersistentVector;
import clojure.lang.Symbol;

import java.util.List;

public final class Stats {

  public static final class Page {
    public final long
      numValues,
      numNonNilValues,
      length,
      dataHeaderLength,
      repetitionLevelsLength,
      definitionLevelsLength,
      dataLength,
      numDictionaryValues,
      dictionaryHeaderLength,
      dictionaryLength;

    Page(long numValues, long numNonNilValues, long length, long dataHeaderLength,
         long repetitionLevelsLength, long definitionLevelsLength, long dataLength,
         long numDictionaryValues, long dictionaryHeaderLength, long dictionaryLength) {
      this.numValues = numValues;
      this.numNonNilValues = numNonNilValues;
      this.length = length;
      this.dataHeaderLength = dataHeaderLength;
      this.repetitionLevelsLength = repetitionLevelsLength;
      this.definitionLevelsLength = definitionLevelsLength;
      this.dataLength = dataLength;
      this.numDictionaryValues = numDictionaryValues;
      this.dictionaryHeaderLength = dictionaryHeaderLength;
      this.dictionaryLength = dictionaryLength;
    }
  }

  public static Page
    createDataPageStats(long numValues, long numNonNilValues, long length, long headerLength,
                        long repetitionLevelsLength, long definitionLevelLength, long dataLength) {
    return new Page(numValues, numNonNilValues, length, headerLength, repetitionLevelsLength,
                    definitionLevelLength, dataLength, 0, 0, 0);
  }

  public static Page createDictionaryPageStats(long numValues, long length,
                                               long dictionaryHeaderLength, long dictionaryLength) {
    return new Page(0, 0, length, 0, 0, 0, 0, numValues, dictionaryHeaderLength, dictionaryLength);
  }

  public static final class ColumnChunk {
    public final long
      numPages,
      numValues,
      numNonNilValues,
      length,
      dataHeaderLength,
      repetitionLevelsLength,
      definitionLevelsLength,
      dataLength,
      numDictionaryValues,
      dictionaryHeaderLength,
      dictionaryLength;

    ColumnChunk(long numPages, long numValues, long numNonNilValues, long length, long dataHeaderLength,
                long repetitionLevelsLength, long definitionLevelsLength, long dataLength,
                long numDictionaryValues, long dictionaryHeaderLength, long dictionaryLength) {
      this.numPages = numPages;
      this.numValues = numValues;
      this.numNonNilValues = numNonNilValues;
      this.length = length;
      this.dataHeaderLength = dataHeaderLength;
      this.repetitionLevelsLength = repetitionLevelsLength;
      this.definitionLevelsLength = definitionLevelsLength;
      this.dataLength = dataLength;
      this.numDictionaryValues = numDictionaryValues;
      this.dictionaryHeaderLength = dictionaryHeaderLength;
      this.dictionaryLength = dictionaryLength;
    }
  }

  public static ColumnChunk createColumnChunkStats(List<Page> pagesStats) {
    long numPages = 0;
    long numValues = 0;
    long numNonNilValues = 0;
    long numDictionaryValues = 0;
    long length = 0;
    long dataHeaderLength = 0;
    long repetitionLevelsLength = 0;
    long definitionLevelsLength = 0;
    long dataLength = 0;
    long dictionaryHeaderLength = 0;
    long dictionaryLength = 0;

    for (Page pageStats : pagesStats) {
      numPages += 1;
      numValues += pageStats.numValues;
      numNonNilValues += pageStats.numNonNilValues;
      numDictionaryValues += pageStats.numDictionaryValues;
      length += pageStats.length;
      dataHeaderLength += pageStats.dataHeaderLength;
      repetitionLevelsLength += pageStats.repetitionLevelsLength;
      definitionLevelsLength += pageStats.definitionLevelsLength;
      dataLength += pageStats.dataLength;
      dictionaryHeaderLength += pageStats.dictionaryHeaderLength;
      dictionaryLength += pageStats.dictionaryLength;
    }

    return new ColumnChunk(numPages, numValues, numNonNilValues, length, dataHeaderLength,
                           repetitionLevelsLength, definitionLevelsLength, dataLength, numDictionaryValues,
                           dictionaryHeaderLength, dictionaryLength);
  }

  public static final class Column {
    public final Symbol
      type,
      encoding,
      compression;

    public final IPersistentVector
      path;

    public final long
      maxRepetitionLevel,
      maxDefinitionLevel,
      numColumnChunks,
      numPages,
      numValues,
      numNonNilValues,
      length,
      dataHeaderLength,
      repetitionLevelsLength,
      definitionLevelsLength,
      dataLength,
      numDictionaryValues,
      dictionaryHeaderLength,
      dictionaryLength;

    Column(Symbol type, Symbol encoding, Symbol compression, long maxRepetitionLevel, long maxDefinitionLevel,
           long numColumnChunks, IPersistentVector path, long numPages, long numValues, long numNonNilValues,
           long length, long dataHeaderLength, long repetitionLevelsLength, long definitionLevelsLength,
           long dataLength, long numDictionaryValues, long dictionaryHeaderLength, long dictionaryLength) {
      this.type = type;
      this.encoding = encoding;
      this.compression = compression;
      this.maxRepetitionLevel = maxRepetitionLevel;
      this.maxDefinitionLevel = maxDefinitionLevel;
      this.numColumnChunks = numColumnChunks;
      this.path = path;
      this.numPages = numPages;
      this.numValues = numValues;
      this.numNonNilValues = numNonNilValues;
      this.length = length;
      this.dataHeaderLength = dataHeaderLength;
      this.repetitionLevelsLength = repetitionLevelsLength;
      this.definitionLevelsLength = definitionLevelsLength;
      this.dataLength = dataLength;
      this.numDictionaryValues = numDictionaryValues;
      this.dictionaryHeaderLength = dictionaryHeaderLength;
      this.dictionaryLength = dictionaryLength;
    }
  }

  public static Column createColumnStats(Symbol type, Symbol encoding, Symbol compression,
                                         int maxRepetitionLevel, int maxDefinitionLevel,
                                         IPersistentVector path,
                                         List<ColumnChunk> columnChunksStats) {
    long numColumnChunks = 0;
    long length = 0;
    long numValues = 0;
    long numNonNilValues = 0;
    long numDictionaryValues = 0;
    long numPages = 0;
    long dataHeaderLength = 0;
    long repetitionLevelsLength = 0;
    long definitionLevelsLength = 0;
    long dataLength = 0;
    long dictionaryHeaderLength = 0;
    long dictionaryLength = 0;

    for (ColumnChunk columnChunkStats : columnChunksStats) {
      numColumnChunks += 1;
      numPages += columnChunkStats.numPages;
      numValues += columnChunkStats.numValues;
      numNonNilValues += columnChunkStats.numNonNilValues;
      numDictionaryValues += columnChunkStats.numDictionaryValues;
      length += columnChunkStats.length;
      dataHeaderLength += columnChunkStats.dataHeaderLength;
      repetitionLevelsLength += columnChunkStats.repetitionLevelsLength;
      definitionLevelsLength += columnChunkStats.definitionLevelsLength;
      dataLength += columnChunkStats.dataLength;
      dictionaryHeaderLength += columnChunkStats.dictionaryHeaderLength;
      dictionaryLength += columnChunkStats.dictionaryLength;
    }

    return new Column(type, encoding, compression, maxRepetitionLevel, maxDefinitionLevel, numColumnChunks,
                      path, numPages, numValues, numNonNilValues, length, dataHeaderLength,
                      repetitionLevelsLength, definitionLevelsLength, dataLength, numDictionaryValues,
                      dictionaryHeaderLength, dictionaryLength);
  }

  public static final class RecordGroup {
    public final long
      numRecords,
      numColumnChunks,
      length,
      dataHeaderLength,
      repetitionLevelsLength,
      definitionLevelsLength,
      dataLength,
      dictionaryHeaderLength,
      dictionaryLength;

    RecordGroup(long numRecords, long numColumnChunks, long length, long dataHeaderLength,
                long repetitionLevelsLength, long definitionLevelsLength, long dataLength,
                long dictionaryHeaderLength, long dictionaryLength) {
      this.numRecords = numRecords;
      this.numColumnChunks = numColumnChunks;
      this.length = length;
      this.dataHeaderLength = dataHeaderLength;
      this.repetitionLevelsLength = repetitionLevelsLength;
      this.definitionLevelsLength = definitionLevelsLength;
      this.dataLength = dataLength;
      this.dictionaryHeaderLength = dictionaryHeaderLength;
      this.dictionaryLength = dictionaryLength;
    }
  }

  public static RecordGroup createRecordGroupStats(long numRecords, List<ColumnChunk> columnChunksStats) {

    long numColumnChunks = 0;
    long length = 0;
    long dataHeaderLength = 0;
    long repetitionLevelsLength = 0;
    long definitionLevelsLength = 0;
    long dataLength = 0;
    long dictionaryHeaderLength = 0;
    long dictionaryLength = 0;

    for (ColumnChunk columnChunkStats : columnChunksStats) {
      numColumnChunks += 1;
      length += columnChunkStats.length;
      dataHeaderLength += columnChunkStats.dataHeaderLength;
      repetitionLevelsLength += columnChunkStats.repetitionLevelsLength;
      definitionLevelsLength += columnChunkStats.definitionLevelsLength;
      dataLength += columnChunkStats.dataLength;
      dictionaryHeaderLength += columnChunkStats.dictionaryHeaderLength;
      dictionaryLength += columnChunkStats.dictionaryLength;
    }

    return new RecordGroup(numRecords, numColumnChunks, length, dataHeaderLength, repetitionLevelsLength,
                           definitionLevelsLength, dataLength, dictionaryHeaderLength, dictionaryLength);
  }

  public static final class Global {
    public final long
      numColumns,
      numRecordGroups,
      numRecords,
      length,
      dataHeaderLength,
      repetitionLevelsLength,
      definitionLevelsLength,
      dataLength,
      dictionaryHeaderLength,
      dictionaryLength,
      metadataLength;

    Global(long numColumns, long numRecordGroups, long numRecords, long length, long dataHeaderLength,
           long repetitionLevelsLength, long definitionLevelsLength, long dataLength,
           long dictionaryHeaderLength, long dictionaryLength, long metadataLength) {
      this.numColumns = numColumns;
      this.numRecordGroups = numRecordGroups;
      this.numRecords = numRecords;
      this.length = length;
      this.dataHeaderLength = dataHeaderLength;
      this.repetitionLevelsLength = repetitionLevelsLength;
      this.definitionLevelsLength = definitionLevelsLength;
      this.dataLength = dataLength;
      this.dictionaryHeaderLength = dictionaryHeaderLength;
      this.dictionaryLength = dictionaryLength;
      this.metadataLength = metadataLength;
    }
  }

  public static Global createGlobalStats(long length, long metadataLength, long numColumns,
                                         List<RecordGroup> recordGroupsStats) {
    long numRecordGroups = 0;
    long numRecords = 0;
    long dataHeaderLength = 0;
    long repetitionLevelsLength = 0;
    long definitionLevelsLength = 0;
    long dataLength = 0;
    long dictionaryHeaderLength = 0;
    long dictionaryLength = 0;

    for (RecordGroup recordGroupStats : recordGroupsStats) {
      numRecordGroups += 1;
      numRecords += recordGroupStats.numRecords;
      dataHeaderLength += recordGroupStats.dataHeaderLength;
      repetitionLevelsLength += recordGroupStats.repetitionLevelsLength;
      definitionLevelsLength += recordGroupStats.definitionLevelsLength;
      dataLength += recordGroupStats.dataLength;
      dictionaryHeaderLength += recordGroupStats.dictionaryHeaderLength;
      dictionaryLength += recordGroupStats.dictionaryLength;
    }

    return new Global(numColumns, numRecordGroups, numRecords, length, dataHeaderLength,
                      repetitionLevelsLength, definitionLevelsLength, dataLength, dictionaryHeaderLength,
                      dictionaryLength, metadataLength);
  }
}
