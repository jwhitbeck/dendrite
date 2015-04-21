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
import clojure.lang.IFn;
import clojure.lang.ITransientCollection;
import clojure.lang.PersistentList;
import clojure.lang.Util;

import java.nio.ByteBuffer;
import java.util.List;

public final class Field implements IWriteable {

  public final static int
    OPTIONAL = 0,
    REQUIRED = 1,
    VECTOR = 2,
    LIST = 3,
    SET = 4,
    MAP = 5;

  public final Keyword name;
  public final int repetition;
  public final int repetitionLevel;
  public final int definitionLevel;
  public final ColumnSpec columnSpec;
  public final List subFields;

  public Field(Keyword name, int repetition, int repetitionLevel, int definitionLevel, ColumnSpec columnSpec,
               List subFields) {
    this.name = name;
    this.repetition = repetition;
    this.repetitionLevel = repetitionLevel;
    this.definitionLevel = definitionLevel;
    this.columnSpec = columnSpec;
    this.subFields = subFields;
  }

  private static List readSubFields(ByteBuffer bb) {
    int n = Bytes.readUInt(bb);
    if (n == 0) {
      return null;
    }
    ITransientCollection subFields = PersistentLinkedSeq.newEmptyTransient();
    for (int i=0; i<n; ++i) {
      subFields = subFields.conj(Field.read(bb));
    }
    return (List) subFields.persistent();
  }

  private void writeSubFieldsTo(MemoryOutputStream mos) {
    if (subFields == null) {
      Bytes.writeUInt(mos, 0);
    } else {
      Bytes.writeUInt(mos, subFields.size());
      for (Object field : subFields) {
        ((Field)field).writeTo(mos);
      }
    }
  }

  @Override
  public void writeTo(MemoryOutputStream mos) {
    byte[] nameBytes = (name == null)? new byte[]{} : Types.toByteArray(Types.toString(name));
    Bytes.writeByteArray(mos, nameBytes);
    Bytes.writeUInt(mos, repetition);
    Bytes.writeUInt(mos, repetitionLevel);
    Bytes.writeUInt(mos, definitionLevel);
    if (columnSpec == null) {
      ColumnSpec.writeNullTo(mos);
    } else {
      columnSpec.writeTo(mos);
    }
    writeSubFieldsTo(mos);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    Field field = (Field) o;
    return Util.equiv(name, field.name) &&
      repetition == field.repetition &&
      repetitionLevel == field.repetitionLevel &&
      definitionLevel == field.definitionLevel &&
      Util.equiv(columnSpec, field.columnSpec) &&
      Util.equiv(subFields, field.subFields);
  }

  public static Field read(ByteBuffer bb) {
    return new Field(Types.toKeyword(Types.toString(Bytes.readByteArray(bb))),
                     Bytes.readUInt(bb),
                     Bytes.readUInt(bb),
                     Bytes.readUInt(bb),
                     ColumnSpec.read(bb),
                     readSubFields(bb));
  }

}
