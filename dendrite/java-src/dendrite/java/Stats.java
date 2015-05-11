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

import clojure.lang.Keyword;
import clojure.lang.IPersistentCollection;
import clojure.lang.IPersistentMap;
import clojure.lang.IPersistentVector;
import clojure.lang.ISeq;
import clojure.lang.ITransientMap;
import clojure.lang.PersistentArrayMap;
import clojure.lang.RT;
import clojure.lang.Symbol;

public final class Stats {

  private final static Keyword
    HEADER_LENGTH = Keyword.intern("header-length"),
    REPETITION_LEVELS_LENGTH = Keyword.intern("repetition-levels-length"),
    DEFINITION_LEVELS_LENGTH = Keyword.intern("definition-levels-length"),
    METADATA_LENGTH = Keyword.intern("metadata-length"),
    DATA_LENGTH = Keyword.intern("data-length"),
    DICTIONARY_HEADER_LENGTH = Keyword.intern("dictionary-header-length"),
    DICTIONARY_LENGTH = Keyword.intern("dictionary-length"),
    NUM_VALUES = Keyword.intern("num-values"),
    LENGTH = Keyword.intern("length"),
    NUM_PAGES = Keyword.intern("num-pages"),
    NUM_DICTIONARY_VALUES = Keyword.intern("num-dictionary-values"),
    TYPE = Keyword.intern("type"),
    ENCODING = Keyword.intern("encoding"),
    COMPRESSION = Keyword.intern("compression"),
    MAX_REPETITION_LEVEL = Keyword.intern("max-repetition-level"),
    MAX_DEFINITION_LEVEL = Keyword.intern("max-definition-level"),
    PATH = Keyword.intern("path"),
    NUM_RECORDS = Keyword.intern("num-records"),
    NUM_RECORD_GROUPS = Keyword.intern("num-record-groups"),
    NUM_COLUMN_CHUNKS = Keyword.intern("num-column-chunks"),
    NUM_COLUMNS = Keyword.intern("num-columns");

  public final static IPersistentMap dataPageStats(int numValues, int length, int headerLength,
                                                   int repetitionLevelsLength, int definitionLevelLength,
                                                   int dataLength) {
    return new PersistentArrayMap(new Object[]{
        NUM_VALUES, numValues,
        LENGTH, length,
        HEADER_LENGTH, headerLength,
        REPETITION_LEVELS_LENGTH, repetitionLevelsLength,
        DEFINITION_LEVELS_LENGTH, definitionLevelLength,
        DATA_LENGTH, dataLength,
      });
  }

  public final static IPersistentMap dictionaryPageStats(int numValues, int length, int dictionaryheaderLength,
                                                         int dictionaryLength) {
    return new PersistentArrayMap(new Object[]{
        NUM_VALUES, numValues,
        LENGTH, length,
        HEADER_LENGTH, dictionaryheaderLength,
        DICTIONARY_LENGTH, dictionaryLength
      });
  }

  public final static IPersistentMap columnChunkStats(IPersistentCollection pageStatsList) {
    int numPages = RT.count(pageStatsList);
    int numValues = 0;
    int numDictionaryValues = 0;
    int length = 0;
    int dataHeaderLength = 0;
    int repetitionLevelsLength = 0;
    int definitionLevelsLength = 0;
    int dataLength = 0;
    int dictionaryHeaderLength = 0;
    int dictionaryLength = 0;

    if (numPages > 0) {
      for (ISeq s = RT.seq(pageStatsList); s != null; s = s.next()) {
        IPersistentMap pageStats = (IPersistentMap)s.first();;
        length += (int)pageStats.valAt(LENGTH);
        boolean isDictionary = pageStats.valAt(DICTIONARY_LENGTH) != null;
        if (isDictionary) {
          numDictionaryValues += (int)pageStats.valAt(NUM_VALUES);
          dictionaryHeaderLength += (int)pageStats.valAt(HEADER_LENGTH);
          dictionaryLength += (int)pageStats.valAt(DICTIONARY_LENGTH);
        } else {
          numValues += (int)pageStats.valAt(NUM_VALUES);
          dataHeaderLength += (int)pageStats.valAt(HEADER_LENGTH);
          dataLength += (int)pageStats.valAt(DATA_LENGTH);
          repetitionLevelsLength += (int)pageStats.valAt(REPETITION_LEVELS_LENGTH);
          definitionLevelsLength += (int)pageStats.valAt(DEFINITION_LEVELS_LENGTH);
        }
      }
    }

    return PersistentArrayMap.EMPTY.asTransient()
      .assoc(NUM_PAGES, numPages)
      .assoc(NUM_VALUES, numValues)
      .assoc(NUM_DICTIONARY_VALUES, numDictionaryValues)
      .assoc(LENGTH, length)
      .assoc(HEADER_LENGTH, dataHeaderLength)
      .assoc(REPETITION_LEVELS_LENGTH, repetitionLevelsLength)
      .assoc(DEFINITION_LEVELS_LENGTH, definitionLevelsLength)
      .assoc(DATA_LENGTH, dataLength)
      .assoc(DICTIONARY_HEADER_LENGTH, dictionaryHeaderLength)
      .assoc(DICTIONARY_LENGTH, dictionaryLength)
      .persistent();
  }

  public static IPersistentMap columnStats(Symbol type, Symbol encoding, Symbol compression,
                                           int maxRepetitionLevel, int maxDefinitionLevel,
                                           IPersistentVector path,
                                           IPersistentCollection columnChunkStatsList) {
    int numColumnChunks = RT.count(columnChunkStatsList);
    int length = 0;
    int numValues = 0;
    int numDictionaryValues = 0;
    int numPages = 0;
    int headerLength = 0;
    int repetitionLevelsLength = 0;
    int definitionLevelsLength = 0;
    int dataLength = 0;
    int dictionaryHeaderLength = 0;
    int dictionaryLength = 0;

    if (numColumnChunks > 0) {
      for (ISeq s = RT.seq(columnChunkStatsList); s != null; s = s.next()) {
        IPersistentMap columnChunkStats = (IPersistentMap)s.first();
        length += (int)columnChunkStats.valAt(LENGTH);
        numValues += (int)columnChunkStats.valAt(NUM_VALUES);
        numDictionaryValues += (int)columnChunkStats.valAt(NUM_DICTIONARY_VALUES);
        numPages += (int)columnChunkStats.valAt(NUM_PAGES);
        headerLength += (int)columnChunkStats.valAt(HEADER_LENGTH);
        repetitionLevelsLength += (int)columnChunkStats.valAt(REPETITION_LEVELS_LENGTH);
        definitionLevelsLength += (int)columnChunkStats.valAt(DEFINITION_LEVELS_LENGTH);
        dataLength += (int)columnChunkStats.valAt(DATA_LENGTH);
        dictionaryHeaderLength += (int)columnChunkStats.valAt(DICTIONARY_HEADER_LENGTH);
        dictionaryLength += (int)columnChunkStats.valAt(DICTIONARY_LENGTH);
      }
    }

    return PersistentArrayMap.EMPTY.asTransient()
      .assoc(TYPE, type)
      .assoc(ENCODING, encoding)
      .assoc(COMPRESSION, compression)
      .assoc(MAX_REPETITION_LEVEL, maxRepetitionLevel)
      .assoc(MAX_DEFINITION_LEVEL, maxDefinitionLevel)
      .assoc(PATH, path)
      .assoc(LENGTH, length)
      .assoc(NUM_VALUES, numValues)
      .assoc(NUM_DICTIONARY_VALUES, numDictionaryValues)
      .assoc(NUM_COLUMN_CHUNKS, numColumnChunks)
      .assoc(NUM_PAGES, numPages)
      .assoc(HEADER_LENGTH, headerLength)
      .assoc(REPETITION_LEVELS_LENGTH, repetitionLevelsLength)
      .assoc(DEFINITION_LEVELS_LENGTH, definitionLevelsLength)
      .assoc(DATA_LENGTH, dataLength)
      .assoc(DICTIONARY_HEADER_LENGTH, dictionaryHeaderLength)
      .assoc(DICTIONARY_LENGTH, dictionaryLength)
      .persistent();
  }

  public static IPersistentMap recordGroupStats(int numRecords, IPersistentCollection columnChunkStatsList) {

    int numColumnChunks = RT.count(columnChunkStatsList);
    int length = 0;
    int headerLength = 0;
    int repetitionLevelsLength = 0;
    int definitionLevelsLength = 0;
    int dataLength = 0;
    int dictionaryHeaderLength = 0;
    int dictionaryLength = 0;

    if (numColumnChunks > 0) {
      for (ISeq s = RT.seq(columnChunkStatsList); s != null; s = s.next()) {
        IPersistentMap columnChunkStats = (IPersistentMap)s.first();
        length += (int)columnChunkStats.valAt(LENGTH);
        headerLength += (int)columnChunkStats.valAt(HEADER_LENGTH);
        repetitionLevelsLength += (int)columnChunkStats.valAt(REPETITION_LEVELS_LENGTH);
        definitionLevelsLength += (int)columnChunkStats.valAt(DEFINITION_LEVELS_LENGTH);
        dataLength += (int)columnChunkStats.valAt(DATA_LENGTH);
        dictionaryHeaderLength += (int)columnChunkStats.valAt(DICTIONARY_HEADER_LENGTH);
        dictionaryLength += (int)columnChunkStats.valAt(DICTIONARY_LENGTH);
      }
    }

    return PersistentArrayMap.EMPTY.asTransient()
      .assoc(LENGTH, length)
      .assoc(NUM_RECORDS, numRecords)
      .assoc(NUM_COLUMN_CHUNKS, numColumnChunks)
      .assoc(HEADER_LENGTH, headerLength)
      .assoc(REPETITION_LEVELS_LENGTH, repetitionLevelsLength)
      .assoc(DEFINITION_LEVELS_LENGTH, definitionLevelsLength)
      .assoc(DATA_LENGTH, dataLength)
      .assoc(DICTIONARY_HEADER_LENGTH, dictionaryHeaderLength)
      .assoc(DICTIONARY_LENGTH, dictionaryLength)
      .persistent();
  }

  public static IPersistentMap globalStats(int length, int numColumns,
                                           IPersistentCollection recordGroupStatsList) {
    int numRecordGroups = RT.count(recordGroupStatsList);
    int dataLength = 0;
    int numRecords = 0;
    int headerLength = 0;
    int repetitionLevelsLength = 0;
    int definitionLevelsLength = 0;
    int dictionaryHeaderLength = 0;
    int dictionaryLength = 0;
    int metadataLength = length;

    if (numRecordGroups > 0) {
      for (ISeq s = RT.seq(recordGroupStatsList); s != null; s = s.next()) {
        IPersistentMap recordGroupStats = (IPersistentMap)s.first();
        metadataLength -= (int)recordGroupStats.valAt(LENGTH);
        numRecords += (int)recordGroupStats.valAt(NUM_RECORDS);
        headerLength += (int)recordGroupStats.valAt(HEADER_LENGTH);
        repetitionLevelsLength += (int)recordGroupStats.valAt(REPETITION_LEVELS_LENGTH);
        definitionLevelsLength += (int)recordGroupStats.valAt(DEFINITION_LEVELS_LENGTH);
        dataLength += (int)recordGroupStats.valAt(DATA_LENGTH);
        dictionaryHeaderLength += (int)recordGroupStats.valAt(DICTIONARY_HEADER_LENGTH);
        dictionaryLength += (int)recordGroupStats.valAt(DICTIONARY_LENGTH);
      }
    }

    return PersistentArrayMap.EMPTY.asTransient()
      .assoc(LENGTH, length)
      .assoc(METADATA_LENGTH, metadataLength)
      .assoc(NUM_RECORDS, numRecords)
      .assoc(NUM_COLUMNS, numColumns)
      .assoc(NUM_RECORD_GROUPS, numRecordGroups)
      .assoc(HEADER_LENGTH, headerLength)
      .assoc(REPETITION_LEVELS_LENGTH, repetitionLevelsLength)
      .assoc(DEFINITION_LEVELS_LENGTH, definitionLevelsLength)
      .assoc(DATA_LENGTH, dataLength)
      .assoc(DICTIONARY_HEADER_LENGTH, dictionaryHeaderLength)
      .assoc(DICTIONARY_LENGTH, dictionaryLength)
      .persistent();
  }

}
