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

  static final PersistentRecord EMPTY
    = new PersistentRecord(null, KeywordIndexHashMap.EMPTY, new Object[]{});
  final KeywordIndexHashMap hm;
  final Object[] ovs;
  final int cnt;
  final IPersistentMap _meta;

  PersistentRecord(IPersistentMap meta, KeywordIndexHashMap hashMap,
                   Object[] orderedValues) {
    hm = hashMap;
    ovs = orderedValues;
    _meta = meta;
    int c = 0;
    for (int i=0; i<ovs.length; ++i) {
      if (ovs[i] != UNDEFINED) {
        c += 1;
      }
    }
    cnt = c;
  }

  public PersistentRecord withMeta(IPersistentMap meta) {
    return new PersistentRecord(meta, hm, ovs);
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
      int i = hm.get((Keyword)key);
      if (i >= 0) {
        if (ovs[i] == UNDEFINED) {
          return null;
        }
        return new MapEntry(hm.kws[i], ovs[i]);
      }
      return null;
    }
    return null;
  }

  @Override
  public boolean containsKey(Object key) {
    if (key instanceof Keyword) {
      int i = hm.get((Keyword)key);
      return i >= 0 && ovs[i] != UNDEFINED;
    }
    return false;
  }

  @Override
  public IPersistentMap empty() {
    return (IPersistentMap) EMPTY.withMeta(_meta);
  }

  @Override
  public int count() {
    return cnt;
  }

  @Override
  public ISeq seq() {
    if (cnt > 0) {
      return new Seq(hm.kws, ovs, cnt, 0, 0);
    }
    return null;
  }

  @Override
  public Object valAt(Object key, Object notFound) {
    if (key instanceof Keyword) {
      int i = hm.get((Keyword)key);
      if (i >= 0) {
        if (ovs[i] == UNDEFINED) {
          return notFound;
        }
        return ovs[i];
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
        while (ovs [i] == UNDEFINED) {
          i += 1;
        }
        MapEntry me = new MapEntry(hm.kws[i], ovs[i]);
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

    final KeywordIndexHashMap hm;

    public Factory(Keyword[] keywords) {
      hm = new KeywordIndexHashMap(keywords);
    }

    public PersistentRecord create(Object [] orderedValues) {
      return new PersistentRecord(null, hm, orderedValues);
    }
  }

  static final class KeywordIndexHashMap implements Serializable {

    final long[] hashArray;
    final Keyword[] kws;
    final int mask;
    static final long hasheqMask  = 0x00000000ffffffffL;
    static final long idxMask     = 0x7fffffff00000000L;
    static final long presenceBit = 0x8000000000000000L;
    static final KeywordIndexHashMap EMPTY = new KeywordIndexHashMap(new Keyword[]{});

    public KeywordIndexHashMap(Keyword[] keywords) {
      kws = keywords;
      int size = hashArraySize(keywords.length);
      hashArray = new long[size];
      mask = size - 1;
      for (int i=0; i<kws.length; ++i) {
        insert(kws[i], i);
      }
    }

    private void insert(Keyword k, long i) {
      int hasheq = k.hasheq();
      long hv = presenceBit | ((long) hasheq & hasheqMask) | ((i << 32) & idxMask);
      int j = hasheq & mask;
      while (hashArray[j] != 0){
        if ((int)(hashArray[j] & hasheqMask) == hasheq) {
          throw new IllegalArgumentException("Duplicate key " + k);
        }
        j = (j + 1) & mask;
      }
      hashArray[j] = hv;
    }

    public int get(Keyword k) {
      int hasheq = k.hasheq();
      long lhasheq = ((long)hasheq) & hasheqMask;
      int j = hasheq & mask;
      long hv = hashArray[j];
      if (hv == 0) {
        return -1;
      }
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

  static class Seq extends ASeq implements Counted {

    final Keyword[] kws;
    final Object[] ovs;
    final int cnt;
    final int i;
    final int c;

    Seq(Keyword[] kws, Object[] ovs, int cnt, int i, int c) {
      this.kws = kws;
      this.ovs = ovs;
      this.cnt = cnt;
      int j = i;
      while (ovs[j] == UNDEFINED) {
        j += 1;
      }
      this.i = j;
      this.c = c;
    }

    Seq(IPersistentMap meta, Keyword[] kws, Object[] ovs, int cnt, int i, int c) {
      super(meta);
      this.kws = kws;
      this.ovs = ovs;
      this.cnt = cnt;
      int j = i;
      while (ovs[j] == UNDEFINED) {
        j += 1;
      }
      this.i = j;
      this.c = c;
    }

    @Override
    public Object first() {
      return new MapEntry(kws[i], ovs[i]);
    }

    @Override
    public ISeq next() {
      if ( c + 1 < cnt ) {
        return new Seq(kws, ovs, cnt, i+1, c+1);
      }
      return null;
    }

    @Override
    public int count() {
      return cnt - c;
    }

    @Override
    public Seq withMeta(IPersistentMap meta) {
      return new Seq(meta, kws, ovs, cnt, i, c);
    }
  }
}
