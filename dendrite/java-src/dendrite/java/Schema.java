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

import clojure.lang.AFn;
import clojure.lang.ArraySeq;
import clojure.lang.EdnReader;
import clojure.lang.IFn;
import clojure.lang.IMapEntry;
import clojure.lang.IObj;
import clojure.lang.IPersistentCollection;
import clojure.lang.IPersistentList;
import clojure.lang.IPersistentMap;
import clojure.lang.IPersistentSet;
import clojure.lang.IPersistentVector;
import clojure.lang.ISeq;
import clojure.lang.ITransientMap;
import clojure.lang.ITransientCollection;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;
import clojure.lang.PersistentList;
import clojure.lang.PersistentHashSet;
import clojure.lang.PersistentVector;
import clojure.lang.RT;
import clojure.lang.Symbol;
import clojure.lang.Util;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.HashSet;

public abstract class Schema implements IWriteable {

  public final static int
    OPTIONAL = 0,
    REQUIRED = 1,
    VECTOR = 2,
    LIST = 3,
    SET = 4,
    MAP = 5;

  private final static String[] repetitionStrings;

  static {
    repetitionStrings = new String[MAP+1];
    repetitionStrings[OPTIONAL] = "optional";
    repetitionStrings[REQUIRED] = "required";
    repetitionStrings[VECTOR] = "vector";
    repetitionStrings[LIST] = "list";
    repetitionStrings[SET] = "set";
    repetitionStrings[MAP] = "map";
  }

  private final static int
    LEAF = 0,
    RECORD = 1,
    COLLECTION = 2;

  public static final Symbol
    COL = Symbol.intern("col"),
    REQ = Symbol.intern("req"),
    SUB_SCHEMA = Symbol.intern("_");

  public static final Keyword
    KEY = Keyword.intern("key"),
    VAL = Keyword.intern("val"),
    TYPE = Keyword.intern("type"),
    DEFAULT = Keyword.intern("default"),
    OLD_TYPE = Keyword.intern("dendrite", "old-type"),
    TAG = Keyword.intern("dendrite", "tag"),
    TAGGED_TYPE = Keyword.intern("dendrite", "tagged"),
    REQUIRED_TYPE = Keyword.intern("dendrite", "required"),
    READERS = Keyword.intern("readers");

  public static boolean isRequired(Object o) {
    IPersistentMap meta = RT.meta(o);
    return meta != null && meta.valAt(TYPE) == REQUIRED_TYPE;
  }

  public static Object req(Object o) {
    if (isRequired(o)) {
      throw new IllegalArgumentException("Cannot mark a field as required multiple times.");
    }
    IPersistentMap meta = RT.meta(o);
    Object oldType = RT.get(meta, TYPE);
    if (oldType != null){
      return ((IObj)o).withMeta(meta.assoc(OLD_TYPE, oldType).assoc(TYPE, REQUIRED_TYPE));
    }
    return ((IObj)o).withMeta((IPersistentMap)RT.assoc(meta, TYPE, REQUIRED_TYPE));
  }

  public static Object unreq(Object o) {
    if (isRequired(o)) {
      IPersistentMap meta = RT.meta(o);
      Object oldType = meta.valAt(OLD_TYPE);
      if (oldType != null) {
        return ((IObj)o).withMeta(meta.without(OLD_TYPE).assoc(TYPE, oldType));
      }
      return ((IObj)o).withMeta(meta.without(TYPE));
    }
    return o;
  }

  private static boolean isTagged(Object o) {
    IPersistentMap meta = RT.meta(o);
    return meta != null && meta.valAt(TYPE) == TAGGED_TYPE;
  }

  public static Object getTag(Object o) {
    IPersistentMap meta = RT.meta(o);
    return meta.valAt(TAG);
  }

  public static Object tag(Object tag, Object o) {
    if (isTagged(o)) {
      throw new IllegalArgumentException("Cannot tag an element multiple times.");
    }
    IPersistentMap meta = RT.meta(o);
    Object oldType = RT.get(meta, TYPE);
    if (oldType != null){
      return ((IObj)o).withMeta(meta.assoc(OLD_TYPE, oldType).assoc(TYPE, TAGGED_TYPE).assoc(TAG, tag));
    }
    return ((IObj)o).withMeta((IPersistentMap)RT.assoc(RT.assoc(meta, TYPE, TAGGED_TYPE), TAG, tag));
  }

  public static Object untag(Object o) {
    if (isTagged(o)) {
      IPersistentMap meta = RT.meta(o);
      Object oldType = meta.valAt(OLD_TYPE);
      if (oldType != null) {
        return ((IObj)o).withMeta(meta.without(OLD_TYPE).assoc(TYPE, oldType).without(TAG));
      }
      return ((IObj)o).withMeta(meta.without(TYPE).without(TAG));
    }
    return o;
  }

  private static IFn parseReq = new AFn() {
      public Object invoke(Object o) {
        return req(o);
      }
    };

  private static IFn parseCol = new AFn() {
      public Object invoke(Object vs) {
        switch (RT.count(vs)) {
        case 1: return new Col((Symbol)RT.first(vs));
        case 2: return new Col((Symbol)RT.first(vs), (Symbol)RT.second(vs));
        case 3: return new Col((Symbol)RT.first(vs), (Symbol)RT.second(vs), (Symbol)RT.third(vs));
        default: throw new IllegalArgumentException(String.format("Invalid col: '%s'.", vs));
        }
      }
    };

  public static Object readString(String s) {
    IPersistentMap opts = new PersistentArrayMap(new Object[]{
        READERS, new PersistentArrayMap(new Object[]{
            REQ, parseReq,
            COL, parseCol})});
    return EdnReader.readString(s, opts);
  }

  public final int repetition;
  public final int repetitionLevel;
  public final int definitionLevel;
  public final IFn fn;

  private Schema(int repetition, int repetitionLevel, int definitionLevel, IFn fn) {
    this.repetition = repetition;
    this.repetitionLevel = repetitionLevel;
    this.definitionLevel = definitionLevel;
    this.fn = fn;
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

  private static void writeCommonFieldsTo(MemoryOutputStream mos, Schema s) {
    mos.write(s.repetition);
    Bytes.writeUInt(mos, s.repetitionLevel);
    Bytes.writeUInt(mos, s.definitionLevel);
  }

  public abstract Schema withFn(IFn fn);

  @Override
  public boolean equals(Object o) {
    if (o == null || !(o instanceof Schema)) {
      return false;
    }
    Schema s = (Schema)o;
    return repetition == s.repetition &&
      repetitionLevel == s.repetitionLevel &&
      definitionLevel == s.definitionLevel &&
      fn == s.fn;
  }

  public static Schema read(ByteBuffer bb) {
    int type = bb.get();
    switch (type) {
    case LEAF: return Leaf.read(bb);
    case RECORD: return Record.read(bb);
    case COLLECTION: return Collection.read(bb);
    default: throw new IllegalStateException("Unknown schema type: " + type);
    }
  }

  public static final class Leaf extends Schema {

    public final int type;
    public final int encoding;
    public final int compression;
    public final int columnIndex;

    public Leaf(int repetition, int repetitionLevel, int definitionLevel, int type, int encoding,
                int compression, int columnIndex, IFn fn) {
      super(repetition, repetitionLevel, definitionLevel, fn);
      this.type = type;
      this.encoding = encoding;
      this.compression = compression;
      this.columnIndex = columnIndex;
    }

    public static Leaf missing() {
      return new Leaf(-REQUIRED, -1, -1, -1, -1, -1, -1, null);
    }

    @Override
    public Leaf withFn(IFn aFn) {
      return new Leaf(repetition, repetitionLevel, definitionLevel, type, encoding, compression,
                      columnIndex, aFn);
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
      return new Leaf(Bytes.readUInt(bb),
                      Bytes.readUInt(bb),
                      Bytes.readUInt(bb),
                      Bytes.readSInt(bb),
                      Bytes.readUInt(bb),
                      Bytes.readUInt(bb),
                      Bytes.readUInt(bb),
                      null);
    }
  }

  public static final class Field implements IWriteable {
    public final Keyword name;
    public final Schema value;

    public Field(Keyword name, Schema value) {
      this.name = name;
      this.value = value;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null) {
        return false;
      }
      Field f = (Field)o;
      return name.equals(f.name) && value.equals(f.value);
    }

    @Override
    public void writeTo(MemoryOutputStream mos) {
      writeName(mos, name);
      mos.write(value);
    }

    public static Field read(ByteBuffer bb) {
      return new Field(readName(bb), Schema.read(bb));
    }
  }

  public static final class Record extends Schema {

    public final int leafColumnIndex;
    public final ISeq fields;

    public Record(int repetition, int repetitionLevel, int definitionLevel, int leafColumnIndex,
                  IPersistentCollection fields, IFn fn) {
      super(repetition, repetitionLevel, definitionLevel, fn);
      this.leafColumnIndex = leafColumnIndex;
      this.fields = RT.seq(fields);
    }

    public static Record missing(Object fields) {
      return new Record(-REQUIRED, -1, -1, -1, RT.seq(fields), null);
    }

    @Override
    public Record withFn(IFn aFn) {
      return new Record(repetition, repetitionLevel, definitionLevel, leafColumnIndex, fields, aFn);
    }

    public Record withFields(IPersistentCollection newFields) {
      return new Record(repetition, repetitionLevel, definitionLevel, leafColumnIndex, newFields, fn);
    }

    public Schema get(Keyword name) {
      for (ISeq s = fields; s != null; s = s.next()) {
        Field field = (Field)s.first();
        if (field.name.equals(name)) {
          return field.value;
        }
      }
      return null;
    }

    private static IPersistentCollection readFields(ByteBuffer bb) {
      int n = Bytes.readUInt(bb);
      if (n == 0) {
        return null;
      }
      ITransientCollection fields = PersistentLinkedSeq.newEmptyTransient();
      for (int i=0; i<n; ++i) {
        fields.conj(Field.read(bb));
      }
      return fields.persistent();
    }

    private void writeFieldsTo(MemoryOutputStream mos) {
      Bytes.writeUInt(mos, RT.count(fields));
      for (ISeq s = RT.seq(fields); s != null; s = s.next()) {
        mos.write((Field)s.first());
      }
    }

    @Override
    public void writeTo(MemoryOutputStream mos) {
      mos.write(RECORD);
      writeCommonFieldsTo(mos, this);
      Bytes.writeUInt(mos, leafColumnIndex);
      writeFieldsTo(mos);
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
      return new Record(Bytes.readUInt(bb),
                        Bytes.readUInt(bb),
                        Bytes.readUInt(bb),
                        Bytes.readUInt(bb),
                        readFields(bb),
                        null);
    }
  }

  public static final class Collection extends Schema {

    public final int leafColumnIndex;
    public final Schema repeatedSchema;

    public Collection(int repetition, int repetitionLevel, int definitionLevel, int leafColumnIndex,
                      Schema repeatedSchema, IFn fn) {
      super(repetition, repetitionLevel, definitionLevel, fn);
      this.leafColumnIndex = leafColumnIndex;
      this.repeatedSchema = repeatedSchema;
    }

    public static Collection missing(int repetition) {
      return new Collection(-repetition, -1, -1, -1, null, null);
    }

    @Override
    public Collection withFn(IFn aFn) {
      return new Collection(repetition, repetitionLevel, definitionLevel, leafColumnIndex,
                            repeatedSchema, aFn);
    }

    public Collection withRepetition(int aRepetition) {
      return new Collection(aRepetition, repetitionLevel, definitionLevel, leafColumnIndex,
                            repeatedSchema, fn);
    }

    public Collection withRepeatedSchema(Schema aRepeatedSchema) {
      return new Collection(repetition, repetitionLevel, definitionLevel, leafColumnIndex,
                            aRepeatedSchema, fn);
    }

    @Override
    public void writeTo(MemoryOutputStream mos) {
      mos.write(COLLECTION);
      writeCommonFieldsTo(mos, this);
      Bytes.writeUInt(mos, leafColumnIndex);
      mos.write(repeatedSchema);
    }

    @Override
    public boolean equals(Object o) {
      if (!super.equals(o) || !(o instanceof Collection)) {
        return false;
      }
      Collection c = (Collection)o;
      return leafColumnIndex == c.leafColumnIndex &&
        repeatedSchema.equals(c.repeatedSchema);
    }

    public static Collection read(ByteBuffer bb) {
      return new Collection(Bytes.readUInt(bb),
                            Bytes.readUInt(bb),
                            Bytes.readUInt(bb),
                            Bytes.readUInt(bb),
                            Schema.read(bb),
                            null);
    }
  }

  private static Object firstKey(Object o) {
    IMapEntry e = (IMapEntry)RT.first(RT.seq(o));
    if (e == null) {
      return null;
    }
    return e.key();
  }

  private static boolean isCol(Object o) {
    return (o instanceof Symbol) || (o instanceof Col);
  }

  private static Col asCol(Object o) {
    if (o instanceof Symbol) {
      return new Col((IPersistentMap)RT.meta(o), (Symbol)o, Types.PLAIN_SYM, Types.NONE_SYM);
    }
    return (Col)o;
  }

  private static boolean isRecord(Object o) {
    return (o instanceof IPersistentMap) && (firstKey(o) instanceof Keyword);
  }

  private static boolean isRepeated(int repetition) {
    return repetition != OPTIONAL && repetition != REQUIRED;
  }

  private final static class SchemaParseException extends RuntimeException {
    SchemaParseException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }

  private static Schema _parse(Types types, IPersistentVector parents, int repLvl, int defLvl,
                               LinkedList<Leaf> leaves, Object o) {
    try {
      if (isCol(o)) {
        return _parseCol(types, parents, repLvl, defLvl, leaves, asCol(o));
      } else if (isRecord(o)) {
        return _parseRecord(types, parents, repLvl, defLvl, leaves, (IPersistentMap)o);
      } else if (o instanceof IPersistentMap) {
        return _parseMap(types, parents, repLvl, defLvl, leaves, (IPersistentMap)o);
      } else if (o instanceof IPersistentCollection) {
        return _parseRepeated(types, parents, repLvl, defLvl, leaves, (IPersistentCollection)o);
      }
      throw new IllegalArgumentException(String.format("Unsupported schema element '%s'", o));
    } catch (SchemaParseException e) {
      throw e;
    } catch (Exception e) {
      throw new SchemaParseException(String.format("Error parsing element at path '%s'", parents), e);
    }
  }

  private static Schema _parseCol(Types types, IPersistentVector parents, int repLvl, int defLvl,
                                  LinkedList<Leaf> leaves, Col col) {
    int type = types.getType(col.type);
    Leaf leaf = new Leaf(isRequired(col)? REQUIRED : OPTIONAL,
                         repLvl,
                         isRequired(col)? defLvl : defLvl + 1,
                         type,
                         types.getEncoding(type, col.encoding),
                         types.getCompression(col.compression),
                         leaves.size(),
                         null);
    leaves.add(leaf);
    return leaf;
  }

  // this works because we are doing a DFS
  private static int getLeafColumnIndex(LinkedList<Leaf> leaves) {
    return leaves.getLast().columnIndex;
  }

  private static Schema _parseRecord(Types types, IPersistentVector parents, int repLvl, int defLvl,
                                     LinkedList<Leaf> leaves, IPersistentMap record) {
    int curDefLvl = isRequired(record)? defLvl : defLvl + 1;
    int repetition = isRequired(record)? REQUIRED : OPTIONAL;
    ITransientCollection fields = PersistentLinkedSeq.newEmptyTransient();
    for (Object o : record) {
      IMapEntry e = (IMapEntry)o;
      Keyword name = (Keyword)e.key();
      fields.conj(new Field(name, _parse(types, parents.cons(name), repLvl, curDefLvl, leaves, e.val())));
    }
    int leafColumnIndex = getLeafColumnIndex(leaves);
    return new Record(repetition, repLvl, curDefLvl, leafColumnIndex, fields.persistent(), null);
  }

  private static int getRepeatedRepetition(Object o) {
    if (o instanceof IPersistentVector) {
      return VECTOR;
    } else if (o instanceof IPersistentSet) {
      return SET;
    } else if (o instanceof IPersistentList) {
      return LIST;
    }
    throw new IllegalArgumentException(String.format("Unsupported repeated schema element '%s'", o));
  }

  private static Schema _parseRepeated(Types types, IPersistentVector parents, int repLvl, int defLvl,
                                       LinkedList<Leaf> leaves, IPersistentCollection coll) {
    if (RT.count(coll) != 1) {
      throw new IllegalArgumentException("Repeated field can only contain a single schema element.");
    }
    Object elem = RT.first(coll);
    if (isRequired(elem)) {
      throw new IllegalArgumentException("Repeated element cannot also be required.");
    }
    Schema repeatedSchema = _parse(types, parents.cons(null), repLvl + 1, defLvl, leaves, elem);
    return new Collection(getRepeatedRepetition(coll),
                          repLvl + 1,
                          defLvl,
                          getLeafColumnIndex(leaves),
                          repeatedSchema,
                          null);
  }

  private static Schema _parseMap(Types types, IPersistentVector parents, int repLvl, int defLvl,
                                  LinkedList<Leaf> leaves, IPersistentMap map) {
    if (RT.count(map) != 1) {
      throw new IllegalArgumentException("Map field can only contain a single key/value schema element.");
    }
    int curRepLvl = repLvl + 1;
    IMapEntry e = (IMapEntry)RT.first(map);
    Schema keyValueRecord = _parseRecord(types, parents.cons(null), curRepLvl, defLvl, leaves,
                                         new PersistentArrayMap(new Object[]{KEY, e.key(), VAL, e.val()}));
    return new Collection(MAP, curRepLvl, defLvl, getLeafColumnIndex(leaves), keyValueRecord, null);
  }

  public static Schema parse(Types types, Object unparsedSchema) {
    try {
      return _parse(types, PersistentVector.EMPTY, 0, 0, new LinkedList<Leaf>(), unparsedSchema);
    } catch (SchemaParseException e) {
      String msg = String.format("Failed to parse schema '%s'. %s.", unparsedSchema, e.getMessage());
      throw new IllegalArgumentException(msg, e.getCause());
    } catch (Exception e) {
      throw new IllegalArgumentException(String.format("Failed to parse schema '%s'.", unparsedSchema), e);
    }
  }

  public static Object unparse(Types types, Schema schema) {
    return _unparse(types, false, schema);
  }

  private static Object _unparse(Types types, boolean asPlain, Schema schema) {
    if (schema instanceof Leaf) {
      return _unparseLeaf(types, asPlain, (Leaf)schema);
    } else if (schema instanceof Record) {
      return _unparseRecord(types, asPlain, (Record)schema);
    } else /* if (schema instanceof Schema.Collection) */ {
      return _unparseCollection(types, asPlain, (Collection)schema);
    }
  }

  private static Object wrapWithRepetition(Object o, int repetition) {
    switch (repetition) {
    case LIST: return new PersistentList(o);
    case VECTOR: return PersistentVector.create(o);
    case SET: return PersistentHashSet.create(o);
    case MAP: return new PersistentArrayMap(new Object[]{RT.get(o, KEY), RT.get(o, VAL)});
    case REQUIRED: return req(o);
    default: return o;
    }
  }

  private static Object _unparseLeaf(Types types, boolean asPlain, Leaf leaf) {
    if (asPlain || (leaf.encoding == Types.PLAIN && leaf.compression == Types.NONE)) {
      return wrapWithRepetition(types.getTypeSymbol(leaf.type), leaf.repetition);
    }
    Col col = new Col(types.getTypeSymbol(leaf.type),
                      types.getEncodingSymbol(leaf.encoding),
                      types.getCompressionSymbol(leaf.compression));

    return wrapWithRepetition(col, leaf.repetition);
  }

  private static Object _unparseRecord(Types types, boolean asPlain, Record record) {
    ITransientMap rec = PersistentArrayMap.EMPTY.asTransient();
    for (ISeq s = record.fields; s != null; s = s.next()) {
      Field field = (Field)s.first();
      rec.assoc(field.name, _unparse(types, asPlain, field.value));
    }
    return wrapWithRepetition(rec.persistent(), record.repetition);
  }

  private static Object _unparseCollection(Types types, boolean asPlain, Collection coll) {
    return wrapWithRepetition(_unparse(types, asPlain, coll.repeatedSchema), coll.repetition);
  }

  public static Object plain(Types types, Object unparsedSchema) {
      return _unparse(types, true, parse(types, unparsedSchema));
  }

  public static Schema subSchema(IPersistentVector entrypoint, Schema schema) {
    ISeq ks = RT.seq(entrypoint);
    Keyword parent = null;
    Schema s = schema;
    while (ks != null && s != null) {
      if (s instanceof Collection) {
        throw new IllegalArgumentException(String.format("Entrypoint '%s' contains repeated field '%s'.",
                                                         entrypoint, parent));
      } else if (s instanceof Leaf) {
        throw new IllegalArgumentException(String.format("Entrypoint '%s' contains leaf node at '%s'.",
                                                         entrypoint, parent));
      } else /* if (s instanceof Record) */{
        parent = (Keyword)ks.first();
        s = ((Record)s).get((Keyword)ks.first());
        ks = ks.next();
      }
    }
    return s;
  }

  private static IFn parseTag = new AFn() {
      public Object invoke(Object tag, Object o) {
        return tag(tag, o);
      }
    };

  public static Object readQueryString(String s) {
    IPersistentMap opts = new PersistentArrayMap(new Object[]{DEFAULT, parseTag});
    return EdnReader.readString(s, opts);
  }

  private final static class QueryContext {
    final Types types;
    final IPersistentMap readers;
    final boolean isMissingFieldsAsNil;
    QueryContext(Types types, IPersistentMap readers, boolean isMissingFieldsAsNil) {
      this.types = types;
      this.readers = readers;
      this.isMissingFieldsAsNil = isMissingFieldsAsNil;
    }
  }

  private static Schema _applyQuery(QueryContext context, Schema schema, Object query,
                                    PersistentVector parents) {
    if (isTagged(query)) {
      return _applyQueryTagged(context, schema, query, parents);
    } else if (isRecord(query)) {
      return _applyQueryRecord(context, schema, (IPersistentMap)query, parents);
    } else if (query instanceof IPersistentMap) {
      return _applyQueryMap(context, schema, (IPersistentMap)query, parents);
    } else if (query instanceof IPersistentSet) {
      return _applyQuerySet(context, schema, (IPersistentSet)query, parents);
    } else if (query instanceof IPersistentVector) {
      return _applyQueryVector(context, schema, (IPersistentVector)query, parents);
    } else if (query instanceof IPersistentList) {
      return _applyQueryList(context, schema, (IPersistentList)query, parents);
    } else if (query instanceof Symbol) {
      return _applyQuerySymbol(context, schema, (Symbol)query, parents);
    }
    throw new IllegalArgumentException(String.format("Unable to parse query element '%s'.", query));
  }

  private static Schema _applyQueryTagged(QueryContext context, Schema schema, Object query,
                                          PersistentVector parents) {
    Symbol tag = (Symbol)getTag(query);
    IFn fn = (IFn)RT.get(context.readers, tag);
    if (fn == null) {
      throw new IllegalArgumentException(String.format("No reader function was provided for tag '%s'.", tag));
    }
    return _applyQuery(context, schema, untag(query), parents).withFn(fn);
  }

  private static HashSet<Keyword> getFieldNameSet(Record record) {
    HashSet<Keyword> set = new HashSet<Keyword>();
    for(ISeq s = RT.seq(record.fields); s != null; s = s.next()) {
      set.add((Keyword)((Field)(s.first())).name);
    }
    return set;
  }

  private static IPersistentMap removeKeys(IPersistentMap m, HashSet<Keyword> keySet) {
    IPersistentMap ret = m;
    for (Keyword k : keySet) {
      ret = ret.without(k);
    }
    return ret;
  }

  private static String missingFieldsErrorMessage(PersistentVector parents,
                                                  IPersistentCollection missingFields) {
    StringBuilder sb = new StringBuilder("The following fields don't exist: ");
    for (ISeq s = RT.seq(missingFields); s != null; s = s.next()) {
      sb.append(parents.cons((Keyword)s.first()));
      if (s.next() != null) {
        sb.append(", ");
      }
    }
    return sb.toString();
  }

  private static Schema _applyQueryRecord(QueryContext context, Schema schema, IPersistentMap query,
                                          PersistentVector parents) {
    if (schema == null) {
      ITransientCollection fields = PersistentLinkedSeq.newEmptyTransient();
      for (ISeq s = RT.seq(query); s != null; s = s.next()) {
        IMapEntry e = (IMapEntry)s.first();
        Keyword name = (Keyword)(e.key());
        fields.conj(new Field(name, _applyQuery(context, null, e.val(), parents.cons(name))));
      }
      return Record.missing(fields.persistent());
    } else if (!(schema instanceof Record)) {
      throw new IllegalArgumentException(String.format("Element at path %s is not a record in schema.",
                                                       parents));
    } else {
      Record record = (Record)schema;
      HashSet<Keyword> availableFieldNames = getFieldNameSet(record);
      IPersistentMap missingFieldsQuery = removeKeys(query, availableFieldNames);
      if (!context.isMissingFieldsAsNil && RT.seq(missingFieldsQuery) != null) {
        throw new IllegalArgumentException(missingFieldsErrorMessage(parents, RT.keys(missingFieldsQuery)));
      }
      ITransientCollection fields = PersistentLinkedSeq.newEmptyTransient();
      for (ISeq s = RT.seq(missingFieldsQuery); s != null; s = s.next()) {
        IMapEntry e = (IMapEntry)s.first();
        Keyword name = (Keyword)(e.key());
        fields.conj(new Field(name, _applyQuery(context, null, e.val(), parents.cons(name))));
      }
      for (ISeq s = RT.seq(record.fields); s != null; s = s.next()) {
        Field field = (Field)s.first();
        if (query.containsKey(field.name)) {
          fields.conj(new Field(field.name, _applyQuery(context, field.value, RT.get(query, field.name),
                                                        parents.cons(field.name))));
        }
      }
      return record.withFields(fields.persistent());
    }
  }

  private static Schema _applyQueryMap(QueryContext context, Schema schema, IPersistentMap query,
                                       PersistentVector parents) {
    if (schema == null) {
      return Collection.missing(MAP);
    } else if (schema.repetition != MAP) {
      throw new IllegalArgumentException(String.format("Element at path %s contains a %s in the schema, " +
                                                       "cannot be read as a map.", parents,
                                                       repetitionStrings[schema.repetition]));
    } else if (RT.count(query) != 1) {
      throw new IllegalArgumentException(String.format("Map query '%s' at path '%s' can only contain " +
                                                       " a single key/value pair", query, parents));
    }
    IMapEntry e = (IMapEntry)RT.first(query);
    Object keyValueQuery = new PersistentArrayMap(new Object[]{KEY, e.key(), VAL, e.val()});
    Collection map = (Collection)schema;
    return map.withRepeatedSchema(_applyQuery(context,
                                              map.repeatedSchema,
                                              keyValueQuery,
                                              parents.cons(null)));
  }

  private static Schema _applyQuerySet(QueryContext context, Schema schema, IPersistentSet query,
                                       PersistentVector parents) {
    if (schema == null) {
      return Collection.missing(SET);
    } else if (schema.repetition != SET) {
      throw new IllegalArgumentException(String.format("Element at path %s contains a %s in the schema, " +
                                                       "cannot be read as a set.", parents,
                                                       repetitionStrings[schema.repetition]));

    }
    return _applyQueryRepeated(context, SET, schema, query, parents);
  }

  private static Schema _applyQueryVector(QueryContext context, Schema schema, IPersistentVector query,
                                          PersistentVector parents) {
    if (schema == null) {
      return Collection.missing(VECTOR);
    } else if (schema.repetition != LIST &&
               schema.repetition != VECTOR &&
               schema.repetition != SET &&
               schema.repetition != MAP) {
      throw new IllegalArgumentException(String.format("Element at path %s contains a %s in the schema, " +
                                                       "cannot be read as a vector.", parents,
                                                       repetitionStrings[schema.repetition]));

    }
    return _applyQueryRepeated(context, VECTOR, schema, query, parents);
  }

  private static Schema _applyQueryList(QueryContext context, Schema schema, IPersistentList query,
                                        PersistentVector parents) {
    if (schema == null) {
      return Collection.missing(LIST);
    } else if (schema.repetition != LIST &&
               schema.repetition != VECTOR &&
               schema.repetition != SET &&
               schema.repetition != MAP) {
      throw new IllegalArgumentException(String.format("Element at path %s contains a %s in the schema, " +
                                                       "cannot be read as a list.", parents,
                                                       repetitionStrings[schema.repetition]));

    }
    return _applyQueryRepeated(context, LIST, schema, query, parents);
  }

  private static Schema _applyQueryRepeated(QueryContext context, int repetition, Schema schema,
                                            IPersistentCollection query, PersistentVector parents) {
    Collection coll = (Collection)schema;
    return coll.withRepetition(repetition)
      .withRepeatedSchema(_applyQuery(context, coll.repeatedSchema, RT.first(query), parents.cons(null)));
  }

  private static Schema _applyQuerySymbol(QueryContext context, Schema schema, Symbol query,
                                          PersistentVector parents) {
    if (schema == null) {
      return Leaf.missing();
    } else if (query.equals(SUB_SCHEMA)) {
      return schema;
    } else if (schema instanceof Record) {
      throw new IllegalArgumentException(String.format("Element at path %s is a record, not a value.",
                                                       parents));
    } else if (schema instanceof Collection) {
      throw new IllegalArgumentException(String.format("Element at path %s is a collection, not a value.",
                                                       parents));
    } else {
      Leaf leaf = (Leaf)schema;
      int queriedType = context.types.getType(query);
      if (leaf.type != queriedType) {
        throw new IllegalArgumentException(String.format("Mismatched column types at path %s. " +
                                                         "Asked for '%s' but schema defines '%s'.",
                                                         parents, query,
                                                         context.types.getTypeSymbol(leaf.type)));
      }
      return leaf;
    }
  }

  public static Schema applyQuery(Types types, boolean isMissingFieldsAsNil, IPersistentMap readers,
                                  Schema schema, Object query) {
    try {
      return _applyQuery(new QueryContext(types, readers, isMissingFieldsAsNil), schema, query,
                         PersistentVector.EMPTY);
    } catch (Exception e) {
      throw new IllegalArgumentException(String.format("Invalid query '%s' for schema '%s'.", query,
                                                       unparse(types, schema)), e);
    }
  }

}