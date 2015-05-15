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

public final class ColumnChunks {

  public static IColumnChunkWriter createWriter(Types types, Schema.Column column,
                                                int targetDataPageLength) {
    switch (column.encoding) {
    case Types.DICTIONARY: return DictionaryColumnChunk.Writer.create(types, column, targetDataPageLength);
    case Types.FREQUENCY: return FrequencyColumnChunk.Writer.create(types, column, targetDataPageLength);
    default: return DataColumnChunk.Writer.create(types, column, targetDataPageLength);
    }
  }

  public static IColumnChunkReader createReader(Types types, ByteBuffer bb,
                                                Metadata.ColumnChunk columnChunkMetadata,
                                                Schema.Column column) {
    switch (column.encoding) {
    case Types.DICTIONARY: return new DictionaryColumnChunk.Reader(types, bb, columnChunkMetadata, column);
    case Types.FREQUENCY: return new FrequencyColumnChunk.Reader(types, bb, columnChunkMetadata, column);
    default: return new DataColumnChunk.Reader(types, bb, columnChunkMetadata, column);
    }
  }
}
