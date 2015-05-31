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
import clojure.lang.ChunkedCons;
import clojure.lang.Cons;
import clojure.lang.IChunk;
import clojure.lang.IChunkedSeq;
import clojure.lang.IFn;
import clojure.lang.IReduce;
import clojure.lang.ISeq;
import clojure.lang.LazySeq;
import clojure.lang.PersistentList;
import clojure.lang.Seqable;
import clojure.lang.Sequential;
import clojure.lang.RT;
import clojure.lang.Util;

import java.util.Iterator;

public abstract class View implements IReduce, ISeq, Seqable, Sequential {

  private ISeq recordSeq = null;
  private boolean isSeqSet = false;
  final int defaultBundleSize;

  View(int defaultBundleSize) {
    this.defaultBundleSize = defaultBundleSize;
  }

  @Override
  public synchronized ISeq seq() {
    if (!isSeqSet) {
      recordSeq = RT.seq(getRecordChunkedSeq(getRecordChunks(defaultBundleSize).iterator()));
      isSeqSet = true;
    }
    return recordSeq;
  }

  @Override
  public Object first() {
    return RT.first(seq());
  }

  @Override
  public ISeq next() {
    return RT.next(seq());
  }

  @Override
  public ISeq more() {
    ISeq next = next();
    if (next == null) {
      return PersistentList.EMPTY;
    }
    return next;
  }

  @Override
  public ISeq empty() {
    return PersistentList.EMPTY;
  }

  @Override
  public ISeq cons(Object o) {
    return new Cons(o, this);
  }

  @Override
  public int count() {
    return RT.count(seq());
  }

  @Override
  public boolean equiv(Object o) {
    return Util.equiv(seq(), o);
  }

  private static Object reduceSeq(IFn f, Object start, ISeq records) {
    Object ret = start;
    ISeq s = RT.seq(records);
    while(s != null) {
      if (s instanceof IChunkedSeq) {
        IChunkedSeq cs = (IChunkedSeq)s;
        ret = cs.chunkedFirst().reduce(f, ret);
        s = RT.seq(cs.chunkedNext());
      } else {
        ret = f.invoke(ret, s.first());
        s = RT.seq(s.next());
      }
    }
    return ret;
  }

  @Override
  public Object reduce(IFn f) {
    ISeq s = RT.seq(seq());
    if (s == null) {
      return f.invoke();
    } else {
      Object first = RT.first(s);
      s = RT.next(s);
      if (s == null) {
        return first;
      } else {
        Object second = RT.first(s);
          Object ret = f.invoke(first, second);
          return reduceSeq(f, ret, RT.next(s));
      }
    }
  }

  @Override
  public Object reduce(IFn f, Object start) {
    return reduceSeq(f, start, seq());
  }

  public Object fold(int n, IFn combinef, IFn reducef) {
    Object init = combinef.invoke();
    Object ret = init;
    for (Object reducedChunkValue : getReducedChunkValues(reducef, init, n)) {
      ret = combinef.invoke(ret, reducedChunkValue);
    }
    return ret;
  }

  protected abstract Iterable<IChunk> getRecordChunks(int bundleSize);

  protected abstract Iterable<Object> getReducedChunkValues(IFn f, Object init, int bundleSize);

  private static ISeq getRecordChunkedSeq(final Iterator<IChunk> recordChunksIterator) {
    return new LazySeq(new AFn() {
        public IChunkedSeq invoke() {
          if (!recordChunksIterator.hasNext()) {
            return null;
          } else {
            return new ChunkedCons(recordChunksIterator.next(), getRecordChunkedSeq(recordChunksIterator));
          }
        }
      });
  }
}
