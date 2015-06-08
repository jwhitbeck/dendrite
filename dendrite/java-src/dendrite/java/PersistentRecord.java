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

import clojure.lang.APersistentMap;
import clojure.lang.ASeq;
import clojure.lang.Counted;
import clojure.lang.IMapEntry;
import clojure.lang.IPersistentMap;
import clojure.lang.ISeq;
import clojure.lang.ITransientMap;
import clojure.lang.Keyword;
import clojure.lang.MapEntry;
import clojure.lang.PersistentArrayMap;

import java.util.Iterator;
import java.io.Serializable;

public class PersistentRecord extends APersistentMap {

  public static final Object UNDEFINED = new Object();

  private static final PersistentRecord EMPTY
    = new PersistentRecord(null, KeywordIndexHashMap.EMPTY, new Object[]{});
  private final KeywordIndexHashMap hashMap;
  private final Object[] orderedValues;
  private final int cnt;
  private final IPersistentMap meta;

  PersistentRecord(IPersistentMap meta, KeywordIndexHashMap hashMap,
                   Object[] orderedValues) {
    this.hashMap = hashMap;
    this.orderedValues = orderedValues;
    this.meta = meta;
    int c = 0;
    for (Object v : orderedValues) {
      if (v != UNDEFINED) {
        c += 1;
      }
    }
    this.cnt = c;
  }

  public PersistentRecord withMeta(IPersistentMap meta) {
    return new PersistentRecord(meta, hashMap, orderedValues);
  }

  IPersistentMap asEditableMap() {
    ITransientMap tm = PersistentArrayMap.EMPTY.asTransient();
    for(Object o : this) {
      MapEntry me = (MapEntry)o;
      tm = tm.assoc(me.key(), me.val());
    }
    return tm.persistent();
  }

  @Override
  public IPersistentMap assoc(Object key, Object val) {
    return asEditableMap().assoc(key, val);
  }

  @Override
  public IPersistentMap assocEx(Object key, Object val) {
    return asEditableMap().assocEx(key, val);
  }

  @Override
  public IPersistentMap without(Object key) {
    return asEditableMap().without(key);
  }

  @Override
  public IMapEntry entryAt(Object key) {
    if (key instanceof Keyword) {
      int i = hashMap.get((Keyword)key);
      if (i >= 0) {
        if (orderedValues[i] == UNDEFINED) {
          return null;
        }
        return new MapEntry(hashMap.keywords[i], orderedValues[i]);
      }
      return null;
    }
    return null;
  }

  @Override
  public boolean containsKey(Object key) {
    if (key instanceof Keyword) {
      int i = hashMap.get((Keyword)key);
      return i >= 0 && orderedValues[i] != UNDEFINED;
    }
    return false;
  }

  @Override
  public IPersistentMap empty() {
    return (IPersistentMap) EMPTY.withMeta(meta);
  }

  @Override
  public int count() {
    return cnt;
  }

  @Override
  public ISeq seq() {
    if (cnt > 0) {
      return new Seq(hashMap.keywords, orderedValues, cnt, 0, 0);
    }
    return null;
  }

  @Override
  public Object valAt(Object key, Object notFound) {
    if (key instanceof Keyword) {
      int i = hashMap.get((Keyword)key);
      if (i >= 0) {
        Object v = orderedValues[i];
        if (v == UNDEFINED) {
          return notFound;
        }
        return v;
      }
      return notFound;
    }
    return notFound;
  }

  @Override
  public Object valAt(Object key) {
    return valAt(key, null);
  }

  @Override
  public Iterator iterator() {
    return new Iterator() {
      int i = 0;
      int c = 0;

      @Override
      public boolean hasNext() {
        return c < cnt;
      }

      @Override
      public Object next() {
        while (orderedValues[i] == UNDEFINED) {
          i += 1;
        }
        MapEntry me = new MapEntry(hashMap.keywords[i], orderedValues[i]);
        i += 1;
        c += 1;
        return me;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  public static final class Factory {

    final KeywordIndexHashMap hashMap;

    public Factory(Keyword[] keywords) {
      this.hashMap = new KeywordIndexHashMap(keywords);
    }

    public PersistentRecord create(Object [] orderedValues) {
      return new PersistentRecord(null, hashMap, orderedValues);
    }
  }

  static final class KeywordIndexHashMap implements Serializable {

    final long[] hashArray;
    final Keyword[] keywords;
    private final int mask;
    private static final long hasheqMask  = 0x00000000ffffffffL;
    private static final long idxMask     = 0x7fffffff00000000L;
    private static final long presenceBit = 0x8000000000000000L;
    static final KeywordIndexHashMap EMPTY = new KeywordIndexHashMap(new Keyword[]{});

    public KeywordIndexHashMap(Keyword[] keywords) {
      this.keywords = keywords;
      int size = hashArraySize(keywords.length);
      hashArray = new long[size];
      mask = size - 1;
      for (int i=0; i<keywords.length; ++i) {
        insert(keywords[i], i);
      }
    }

    private void insert(Keyword k, long i) {
      int hasheq = k.hasheq();
      int j = hasheq & mask;
      while (hashArray[j] != 0){
        if ((int)(hashArray[j] & hasheqMask) == hasheq) {
          throw new IllegalArgumentException("Duplicate key " + k);
        }
        j = (j + 1) & mask;
      }
      long hv = presenceBit | ((long) hasheq & hasheqMask) | ((i << 32) & idxMask);
      hashArray[j] = hv;
    }

    public int get(Keyword k) {
      int hasheq = k.hasheq();
      int j = hasheq & mask;
      long hv = hashArray[j];
      if (hv == 0) {
        return -1;
      }
      long lhasheq = ((long)hasheq) & hasheqMask;
      while ((hv & hasheqMask) != lhasheq) {
        j = (j + 1) & mask;
        hv = hashArray[j];
        if (hv == 0) {
          return -1;
        }
      }
      return (int)((hv & idxMask) >>> 32);
    }

    private static int hashArraySize(int kwCnt) {
      int s = 1;
      while (s < kwCnt) {
        s <<= 1;
      }
      return s << 1;
    }
  }

  private static class Seq extends ASeq implements Counted {

    private final Keyword[] keywords;
    private final Object[] orderedValues;
    private final int cnt;
    private final int i;
    private final int c;

    Seq(Keyword[] keywords, Object[] orderedValues, int cnt, int i, int c) {
      this.keywords = keywords;
      this.orderedValues = orderedValues;
      this.cnt = cnt;
      int j = i;
      while (orderedValues[j] == UNDEFINED) {
        j += 1;
      }
      this.i = j;
      this.c = c;
    }

    Seq(IPersistentMap meta, Keyword[] keywords, Object[] orderedValues, int cnt, int i, int c) {
      super(meta);
      this.keywords = keywords;
      this.orderedValues = orderedValues;
      this.cnt = cnt;
      int j = i;
      while (orderedValues[j] == UNDEFINED) {
        j += 1;
      }
      this.i = j;
      this.c = c;
    }

    @Override
    public Object first() {
      return new MapEntry(keywords[i], orderedValues[i]);
    }

    @Override
    public ISeq next() {
      if (c + 1 < cnt) {
        return new Seq(keywords, orderedValues, cnt, i+1, c+1);
      }
      return null;
    }

    @Override
    public int count() {
      return cnt - c;
    }

    @Override
    public Seq withMeta(IPersistentMap meta) {
      return new Seq(meta, keywords, orderedValues, cnt, i, c);
    }
  }
}
