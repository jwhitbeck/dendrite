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

public abstract class SchemaNode implements IWriteable {

  public final static int
    MISSING = -1,
    OPTIONAL = 0,
    REQUIRED = 1,
    VECTOR = 2,
    LIST = 3,
    SET = 4,
    MAP = 5;

  public final static int
    LEAF = 0,
    RECORD = 1,
    COLLECTION = 2;

  public final Keyword name;
  public final int repetition;
  public final int repetitionLevel;
  public final int definitionLevel;
  public final IFn mapFn;

  private SchemaNode(Keyword name, int repetition, int repetitionLevel, int definitionLevel, IFn mapFn) {
    this.name = name;
    this.repetition = repetition;
    this.repetitionLevel = repetitionLevel;
    this.definitionLevel = definitionLevel;
    this.mapFn = mapFn;
  }

  private static void writeName(MemoryOutputStream mos, Keyword name) {
    byte[] nameBytes = (name == null)? null : Types.toByteArray(Types.toString(name));
    Bytes.writeByteArray(mos, nameBytes);
  }

  private static Keyword readName(ByteBuffer bb) {
    byte[] nameBytes = Bytes.readByteArray(bb);
    if (nameBytes == null) {
      return null;
    }
    return Types.toKeyword(Types.toString(nameBytes));
  }

  private static void writeCommonFieldsTo(MemoryOutputStream mos, SchemaNode sn) {
    writeName(mos, sn.name);
    mos.write(sn.repetition);
    Bytes.writeUInt(mos, sn.repetitionLevel);
    Bytes.writeUInt(mos, sn.definitionLevel);
  }

  public boolean isRepeated() {
    return repetition != OPTIONAL && repetition != REQUIRED;
  }

  public boolean isRequired() {
    return repetition == REQUIRED;
  }

  public boolean isMissing() {
    return repetition == MISSING;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || !(o instanceof SchemaNode)) {
      return false;
    }
    SchemaNode sn = (SchemaNode)o;
    return Util.equiv(name, sn.name) &&
      repetition == sn.repetition &&
      repetitionLevel == sn.repetitionLevel &&
      definitionLevel == sn.definitionLevel &&
      mapFn == sn.mapFn;
  }

  public static SchemaNode read(ByteBuffer bb) {
    int nodeType = bb.get();
    switch (nodeType) {
    case LEAF: return Leaf.read(bb);
    case RECORD: return Record.read(bb);
    case COLLECTION: return Collection.read(bb);
    default: throw new IllegalStateException("Unknown schema node type: " + nodeType);
    }
  }

  public static final class Leaf extends SchemaNode {
    public final int type;
    public final int encoding;
    public final int compression;
    public final int columnIndex;

    public Leaf(Keyword name, int repetition, int repetitionLevel, int definitionLevel,
                int type, int encoding, int compression, int columnIndex, IFn mapFn) {
      super(name, repetition, repetitionLevel, definitionLevel, mapFn);
      this.type = type;
      this.encoding = encoding;
      this.compression = compression;
      this.columnIndex = columnIndex;
    }

    public Leaf withMapFn(IFn fn) {
      return new Leaf(name, repetition, repetitionLevel, definitionLevel, type, encoding, compression,
                      columnIndex, fn);
    }

    @Override
    public void writeTo(MemoryOutputStream mos) {
      mos.write(LEAF);
      writeCommonFieldsTo(mos, this);
      Bytes.writeSInt(mos, type);
      Bytes.writeUInt(mos, encoding);
      Bytes.writeUInt(mos, compression);
      Bytes.writeUInt(mos, columnIndex);
    }

    @Override
    public boolean equals(Object o) {
      if (!super.equals(o) || !(o instanceof Leaf)) {
        return false;
      }
      Leaf l = (Leaf)o;
      return type == l.type &&
        encoding == l.encoding &&
        compression == l.compression &&
        columnIndex == l.columnIndex;
    }

    public static Leaf read(ByteBuffer bb) {
      return new Leaf(readName(bb),
                      Bytes.readUInt(bb),
                      Bytes.readUInt(bb),
                      Bytes.readUInt(bb),
                      Bytes.readSInt(bb),
                      Bytes.readUInt(bb),
                      Bytes.readUInt(bb),
                      Bytes.readUInt(bb),
                      null);
    }
  }

  private static List readList(ByteBuffer bb) {
    int n = Bytes.readUInt(bb);
    if (n == 0) {
      return null;
    }
    ITransientCollection list = PersistentLinkedSeq.newEmptyTransient();
    for (int i=0; i<n; ++i) {
      list.conj(SchemaNode.read(bb));
    }
    return (List) list.persistent();
  }

  private static void writeListTo(MemoryOutputStream mos, List list) {
    if (list == null) {
      Bytes.writeUInt(mos, 0);
    } else {
      Bytes.writeUInt(mos, list.size());
      for (Object o : list) {
        mos.write((SchemaNode)o);
      }
    }
  }

  public static final class Record extends SchemaNode {

    public final int leafColumnIndex;
    public final List fields;

    public Record(Keyword name, int repetition, int repetitionLevel, int definitionLevel,
                  int leafColumnIndex, List fields, IFn mapFn) {
      super(name, repetition, repetitionLevel, definitionLevel, mapFn);
      this.leafColumnIndex = leafColumnIndex;
      this.fields = fields;
    }

    public Record withMapFn(IFn fn) {
      return new Record(name, repetition, repetitionLevel, definitionLevel, leafColumnIndex, fields, fn);
    }

    public SchemaNode get(Keyword name) {
      for (Object o : fields) {
        SchemaNode node = (SchemaNode)o;
        if (node.name.equals(name)) {
          return node;
        }
      }
      return null;
    }

    @Override
    public void writeTo(MemoryOutputStream mos) {
      mos.write(RECORD);
      writeCommonFieldsTo(mos, this);
      Bytes.writeUInt(mos, leafColumnIndex);
      writeListTo(mos, fields);
    }

    @Override
    public boolean equals(Object o) {
      if (!super.equals(o) || !(o instanceof Record)) {
        return false;
      }
      Record r = (Record)o;
      return leafColumnIndex == r.leafColumnIndex &&
        Util.equiv(fields, r.fields);
    }

    public static Record read(ByteBuffer bb) {
      return new Record(readName(bb),
                        Bytes.readUInt(bb),
                        Bytes.readUInt(bb),
                        Bytes.readUInt(bb),
                        Bytes.readUInt(bb),
                        readList(bb),
                        null);
    }
  }

  public static final class Collection extends SchemaNode {

    public final int leafColumnIndex;
    public final SchemaNode repeatedNode;

    public Collection(Keyword name, int repetition, int repetitionLevel, int definitionLevel,
                      int leafColumnIndex, SchemaNode repeatedNode, IFn mapFn) {
      super(name, repetition, repetitionLevel, definitionLevel, mapFn);
      this.leafColumnIndex = leafColumnIndex;
      this.repeatedNode = repeatedNode;
    }

    public Collection withMapFn(IFn fn) {
      return new Collection(name, repetition, repetitionLevel, definitionLevel, leafColumnIndex, repeatedNode,
                            fn);
    }

    @Override
    public void writeTo(MemoryOutputStream mos) {
      mos.write(COLLECTION);
      writeCommonFieldsTo(mos, this);
      Bytes.writeUInt(mos, leafColumnIndex);
      mos.write(repeatedNode);
    }

    @Override
    public boolean equals(Object o) {
      if (!super.equals(o) || !(o instanceof Collection)) {
        return false;
      }
      Collection c = (Collection)o;
      return leafColumnIndex == c.leafColumnIndex &&
        repeatedNode.equals(c.repeatedNode);
    }

    public static Collection read(ByteBuffer bb) {
      return new Collection(readName(bb),
                            Bytes.readUInt(bb),
                            Bytes.readUInt(bb),
                            Bytes.readUInt(bb),
                            Bytes.readUInt(bb),
                            SchemaNode.read(bb),
                            null);
    }

  }



}
