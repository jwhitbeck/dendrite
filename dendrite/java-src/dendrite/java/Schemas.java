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

import java.util.LinkedList;
import java.util.List;

public final class Schemas {

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
    TAGGED = Keyword.intern("dendrite", "tagged"),
    REQUIRED = Keyword.intern("dendrite", "required"),
    READERS = Keyword.intern("readers");

  private static boolean isRequired(Object o) {
    IPersistentMap meta = RT.meta(o);
    return meta != null && meta.valAt(TYPE) == REQUIRED;
  }

  public static Object req(Object o) {
    if (isRequired(o)) {
      throw new IllegalArgumentException("Cannot mark a field as required multiple times.");
    }
    IPersistentMap meta = RT.meta(o);
    Object oldType = RT.get(meta, TYPE);
    if (oldType != null){
      return ((IObj)o).withMeta(meta.assoc(OLD_TYPE, oldType).assoc(TYPE, REQUIRED));
    }
    return ((IObj)o).withMeta((IPersistentMap)RT.assoc(meta, TYPE, REQUIRED));
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
    return meta != null && meta.valAt(TYPE) == TAGGED;
  }

  public static Object getTag(Object o) {
    IPersistentMap meta = RT.meta(o);
    return meta.valAt(TAG);
  }

  public static Object tag(Object tag, Object o) {
    if (isTagged(o)) {
      throw new IllegalArgumentException("Cannot tag and element multiple times.");
    }
    IPersistentMap meta = RT.meta(o);
    Object oldType = RT.get(meta, TYPE);
    if (oldType != null){
      return ((IObj)o).withMeta(meta.assoc(OLD_TYPE, oldType).assoc(TYPE, TAGGED).assoc(TAG, tag));
    }
    return ((IObj)o).withMeta((IPersistentMap)RT.assoc(RT.assoc(meta, TYPE, TAGGED), TAG, tag));
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
    return repetition != SchemaNode.OPTIONAL && repetition != SchemaNode.REQUIRED;
  }

  private static int resolveRepetition(int parentRepetition, int childRepetition) {
    if (isRepeated(parentRepetition)) {
      if (childRepetition == SchemaNode.REQUIRED) {
        throw new IllegalArgumentException("Repeated field cannot be marked as required.");
      } else if (childRepetition == SchemaNode.OPTIONAL) {
        return parentRepetition;
      }
    }
    return childRepetition;
  }

  private static Object last(IPersistentVector vec) {
    if (vec.count() == 0) {
      return null;
    }
    return vec.nth(vec.count()-1);
  }

  private final static class SchemaException extends RuntimeException {
    SchemaException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }

  public static SchemaNode parse(Types types, Object unparsedSchema) {
    try {
      if (getRepetition(unparsedSchema) == SchemaNode.OPTIONAL) {
        unparsedSchema = req(unparsedSchema);
      }
      return _parse(types, PersistentVector.EMPTY, SchemaNode.OPTIONAL, 0, 0,
                    new LinkedList<SchemaNode.Leaf>(), unparsedSchema);
    } catch (SchemaException e) {
      String msg = String.format("Failed to parse schema '%s'. %s.", unparsedSchema, e.getMessage());
      throw new IllegalArgumentException(msg, e.getCause());
    } catch (Exception e) {
      throw new IllegalArgumentException(String.format("Failed to parse schema '%s'.", unparsedSchema), e);
    }
  }

  private static SchemaNode _parse(Types types, IPersistentVector parents, int repetition,
                                   int repLvl, int defLvl, LinkedList<SchemaNode.Leaf> leaves, Object o) {
    try {
      if (isCol(o)) {
        return _parseCol(types, parents, repetition, repLvl, defLvl, leaves, asCol(o));
      } else if (isRecord(o)) {
        return _parseRecord(types, parents, repetition, repLvl, defLvl, leaves, (IPersistentMap)o);
      } else if (o instanceof IPersistentMap) {
        return _parseMap(types, parents, repetition, repLvl, defLvl, leaves, (IPersistentMap)o);
      } else if (o instanceof IPersistentCollection) {
        return _parseRepeated(types, parents, repetition, repLvl, defLvl, leaves,
                              (IPersistentCollection)o);
      }
      throw new IllegalArgumentException(String.format("Unsupported schema element '%s'", o));
    } catch (SchemaException e) {
      throw e;
    } catch (Exception e) {
      throw new SchemaException(String.format("Error parsing element at path '%s'", parents), e);
    }
  }

  private static SchemaNode _parseCol(Types types, IPersistentVector parents, int repetition, int repLvl,
                                      int defLvl, LinkedList<SchemaNode.Leaf> leaves, Col col) {
    int columnIndex = leaves.size();
    int curRepLvl = isRepeated(repetition)? repLvl +1 : repLvl;
    int curDefLvl = isRequired(col)? defLvl : defLvl + 1;
    int curRepetition = resolveRepetition(repetition,
                                          isRequired(col)? SchemaNode.REQUIRED : SchemaNode.OPTIONAL);
    int type = types.getType(col.type);
    int encoding = types.getEncoding(type, col.encoding);
    int compression = types.getCompression(col.compression);
    Keyword name = (Keyword)last(parents);
    SchemaNode.Leaf leaf = new SchemaNode.Leaf(name, curRepetition, curRepLvl, curDefLvl, type,
                                               encoding, compression, columnIndex, null);
    leaves.add(leaf);
    return leaf;
  }

  // this works because we are doing a DFS
  private static int getLeafColumnIndex(LinkedList<SchemaNode.Leaf> leaves) {
    return leaves.getLast().columnIndex;
  }

  private static SchemaNode _parseRecord(Types types, IPersistentVector parents, int repetition, int repLvl,
                                         int defLvl, LinkedList<SchemaNode.Leaf> leaves,
                                         IPersistentMap record) {
    int curRepLvl = isRepeated(repetition)? repLvl + 1 :repLvl;
    int curDefLvl = isRequired(record)? defLvl : defLvl + 1;
    int curRepetition = resolveRepetition(repetition,
                                          isRequired(record)? SchemaNode.REQUIRED : SchemaNode.OPTIONAL);
    ITransientCollection _fields = PersistentLinkedSeq.newEmptyTransient();
    for (Object o : record) {
      IMapEntry e = (IMapEntry)o;
      _fields.conj(_parse(types, parents.cons(e.key()), SchemaNode.OPTIONAL, curRepLvl, curDefLvl,
                          leaves, e.val()));
    }
    Keyword name = (Keyword)last(parents);
    List fields = (List) _fields.persistent();
    int leafColumnIndex = getLeafColumnIndex(leaves);
    return new SchemaNode.Record(name, curRepetition, curRepLvl, curDefLvl, leafColumnIndex, fields, null);
  }

  private static int getRepetition(Object o) {
    if (isCol(o) || isRecord(o)) {
      return isRequired(o) ? SchemaNode.REQUIRED : SchemaNode.OPTIONAL;
    } else if (isRequired(o)) {
      throw new IllegalArgumentException("Repeated field cannot be marked as required.");
    } else if (o instanceof IPersistentVector) {
      return SchemaNode.VECTOR;
    } else if (o instanceof IPersistentSet) {
      return SchemaNode.SET;
    } else if (o instanceof IPersistentList) {
      return SchemaNode.LIST;
    } else if (o instanceof IPersistentMap) {
      return SchemaNode.MAP;
    }
    throw new IllegalArgumentException(String.format("Unsupported schema element '%s'", o));
  }

  private static SchemaNode _parseRepeated(Types types, IPersistentVector parents, int repetition, int repLvl,
                                           int defLvl, LinkedList<SchemaNode.Leaf> leaves,
                                           IPersistentCollection coll) {
    int curRepetition = getRepetition(coll);
    if (RT.count(coll) != 1) {
      throw new IllegalArgumentException("Repeated field can only contain a single schema element.");
    }
    Object elem = RT.first(coll);
    if (isRepeated(getRepetition(elem))) {
      int curRepLvl = repLvl + 1;
      int curDefLvl = defLvl + 1;
      Keyword name = (Keyword)last(parents);
      SchemaNode subSchema = _parse(types, parents.cons(null), curRepetition, curRepLvl, curDefLvl,
                                    leaves, elem);
      int leafColumnIndex = getLeafColumnIndex(leaves);
      return new SchemaNode.Collection(name, curRepetition, curRepLvl, curDefLvl, leafColumnIndex,
                                       subSchema, null);
    }
    return _parse(types, parents, curRepetition, repLvl, defLvl, leaves, elem);
  }

  private static SchemaNode _parseMap(Types types, IPersistentVector parents, int repetition, int repLvl,
                                      int defLvl, LinkedList<SchemaNode.Leaf> leaves, IPersistentMap map) {
    if (RT.count(map) != 1) {
      throw new IllegalArgumentException("Map field can only contain a single key/value schema element.");
    }
    if (isRequired(map)) {
      throw new IllegalArgumentException("Map field cannot be marked as required.");
    }
    int curRepLvl = repLvl + 1;
    int curDefLvl = defLvl + 1;
    IMapEntry e = (IMapEntry)RT.first(map);
    SchemaNode keySchema = _parse(types, parents.cons(KEY), SchemaNode.OPTIONAL, curRepLvl, curDefLvl,
                                  leaves, e.key());
    SchemaNode valSchema = _parse(types, parents.cons(VAL), SchemaNode.OPTIONAL, curRepLvl, curDefLvl,
                                  leaves, e.val());
    List fields = ArraySeq.create(keySchema, valSchema);
    int leafColumnIndex = getLeafColumnIndex(leaves);
    Keyword name = (Keyword)last(parents);
    return new SchemaNode.Record(name, SchemaNode.MAP, curRepLvl, curDefLvl, leafColumnIndex, fields, null);
  }

  public static Object unparse(Types types, SchemaNode schema) {
    return unreq(_unparse(types, false, schema));
  }

  private static Object _unparse(Types types, boolean asPlain, SchemaNode schema) {
    if (schema instanceof SchemaNode.Leaf) {
      return _unparseLeaf(types, asPlain, (SchemaNode.Leaf)schema);
    } else if (schema instanceof SchemaNode.Record) {
      return _unparseRecord(types, asPlain, (SchemaNode.Record)schema);
    } else /* if (schema instanceof SchemaNode.Collection) */ {
      return _unparseCollection(types, asPlain, (SchemaNode.Collection)schema);
    }
  }

  private static Object wrapWithRepetition(Object o, int repetition) {
    switch (repetition) {
    case SchemaNode.LIST: return new PersistentList(o);
    case SchemaNode.VECTOR: return PersistentVector.create(o);
    case SchemaNode.SET: return PersistentHashSet.create(o);
    case SchemaNode.MAP: return new PersistentArrayMap(new Object[]{RT.get(o, KEY), RT.get(o, VAL)});
    case SchemaNode.REQUIRED: return req(o);
    default: return o;
    }
  }

  private static Object _unparseLeaf(Types types, boolean asPlain, SchemaNode.Leaf leaf) {
    if (asPlain || (leaf.encoding == Types.PLAIN && leaf.compression == Types.NONE)) {
      return wrapWithRepetition(types.getTypeSymbol(leaf.type), leaf.repetition);
    }
    Col col = new Col(types.getTypeSymbol(leaf.type),
                      types.getEncodingSymbol(leaf.encoding),
                      types.getCompressionSymbol(leaf.compression));

    return wrapWithRepetition(col, leaf.repetition);
  }

  private static Object _unparseRecord(Types types, boolean asPlain, SchemaNode.Record record) {
    ITransientMap rec = PersistentArrayMap.EMPTY.asTransient();
    for (Object o : record.fields) {
      SchemaNode s = (SchemaNode)o;
      rec.assoc(s.name, _unparse(types, asPlain, s));
    }
    return wrapWithRepetition(rec.persistent(), record.repetition);
  }

  private static Object _unparseCollection(Types types, boolean asPlain, SchemaNode.Collection coll) {
    return wrapWithRepetition(_unparse(types, asPlain, coll.repeatedNode), coll.repetition);
  }

  public static Object plain(Types types, Object unparsedSchema) {
      return _unparse(types, true, parse(types, unparsedSchema));
  }

  public static SchemaNode subSchema(IPersistentVector entrypoint, SchemaNode schema) {
    ISeq ks = RT.seq(entrypoint);
    SchemaNode node = schema;
    while (ks != null && node != null) {
      if (node.isRepeated()) {
        throw new IllegalArgumentException(String.format("Entrypoint '%s' contains repeated field '%s'.",
                                                         entrypoint, node.name));
      }
      if (node instanceof SchemaNode.Record) {
        node = ((SchemaNode.Record)node).get((Keyword)ks.first());
        ks = ks.next();
      } else if (node instanceof SchemaNode.Leaf) {
        throw new IllegalArgumentException(String.format("Entrypoint '%s' contains leaf node at '%s'.",
                                                         entrypoint, node.name));
      }
    }
    return node;
  }

  private static IFn parseTag = new AFn() {
      public Object invoke(Object tag, Object o) {
        return tag(tag, o);
      }
    };

  public static Object readQueryString(String s) {
    IPersistentMap opts = new PersistentArrayMap(new Object[]{
        READERS, new PersistentArrayMap(new Object[]{
            DEFAULT, parseTag})});
    return EdnReader.readString(s, opts);
  }

  private final static class QueryOpts {
    final IPersistentMap readers;
    final boolean missingFieldsAsNil;
    QueryOpts(IPersistentMap readers, boolean missingFieldsAsNil) {
      this.readers = readers;
      this.missingFieldsAsNil = missingFieldsAsNil;
    }
  }

  private static SchemaNode _applyQuery(QueryOpts opts, SchemaNode schema, Object query,
                                        PersistentVector parents) {
    if (isTagged(query)) {
      return _applyQueryTagged(opts, schema, query, parents);
    } else if (isRecord(query)) {
      return _applyQueryRecord(opts, schema, query, parents);
    } else if (query instanceof IPersistentMap) {
      return _applyQueryMap(opts, schema, query, parents);
    } else if (query instanceof IPersistentSet) {
      return _applyQuerySet(opts, schema, query, parents);
    } else if (query instanceof IPersistentVector) {
      return _applyQueryVector(opts, schema, query, parents);
    } else if (query instanceof IPersistentList) {
      return _applyQueryList(opts, schema, query, parents);
    } else if (query instanceof Symbol) {
      return _applyQuerySymbol(opts, schema, query, parents);
    }
    throw new IllegalArgumentException(String.format("Unable to parse query element '%s'.", query));
  }

  private static SchemaNode _applyQueryTagged(QueryOpts opts, SchemaNode schema, Object query,
                                              PersistentVector parents) {
    return null;
  }

  private static SchemaNode _applyQueryRecord(QueryOpts opts, SchemaNode schema, Object query,
                                              PersistentVector parents) {
    return null;
  }

  private static SchemaNode _applyQueryMap(QueryOpts opts, SchemaNode schema, Object query,
                                           PersistentVector parents) {
    return null;
  }

  private static SchemaNode _applyQuerySet(QueryOpts opts, SchemaNode schema, Object query,
                                           PersistentVector parents) {
    return null;
  }

  private static SchemaNode _applyQueryVector(QueryOpts opts, SchemaNode schema, Object query,
                                              PersistentVector parents) {
    return null;
  }

  private static SchemaNode _applyQueryList(QueryOpts opts, SchemaNode schema, Object query,
                                            PersistentVector parents) {
    return null;
  }

  private static SchemaNode _applyQuerySymbol(QueryOpts opts, SchemaNode schema, Object query,
                                              PersistentVector parents) {
    return null;
  }

}
