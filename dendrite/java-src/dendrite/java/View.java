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
import clojure.lang.Agent;
import clojure.lang.ChunkedCons;
import clojure.lang.Cons;
import clojure.lang.IChunk;
import clojure.lang.IChunkedSeq;
import clojure.lang.IFn;
import clojure.lang.IReduce;
import clojure.lang.ISeq;
import clojure.lang.LazySeq;
import clojure.lang.PersistentList;
import clojure.lang.RT;
import clojure.lang.Reduced;
import clojure.lang.Seqable;
import clojure.lang.Sequential;
import clojure.lang.Util;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

public abstract class View implements IReduce, ISeq, Seqable, Sequential {

  private ISeq seq = null;
  private boolean isSeqSet = false;
  protected final int defaultBundleSize;

  View(int defaultBundleSize) {
    this.defaultBundleSize = defaultBundleSize;
  }

  @Override
  public synchronized ISeq seq() {
    if (!isSeqSet) {
      seq = RT.seq(getChunkedSeq(getChunks(defaultBundleSize).iterator()));
      isSeqSet = true;
    }
    return seq;
  }

  public synchronized boolean isSeqSet() {
    return isSeqSet;
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
    IChunkedSeq s = (IChunkedSeq)RT.seq(records);
    while (s != null) {
      ret = s.chunkedFirst().reduce(f, ret);
      if (RT.isReduced(ret)) {
        return ((Reduced)ret).deref();
      }
      s = (IChunkedSeq)RT.seq(s.chunkedNext());
    }
    return ret;
  }

  private static Object reduceIterator(IFn f, Object start, Iterator<Object> chunksIterator) {
    Object ret = start;
    while (chunksIterator.hasNext()) {
      IChunk chunk = (IChunk)chunksIterator.next();
      if (chunk.count() > 0) {
        ret = chunk.reduce(f, ret);
        if (RT.isReduced(ret)) {
          return ((Reduced)ret).deref();
        }
      }
    }
    return ret;
  }

  @Override
  public Object reduce(IFn f) {
    if (isSeqSet()) {
      ISeq s = RT.seq(seq());
      if (s == null) {
        return f.invoke();
      } else {
        Object ret = RT.first(s);
        s = RT.next(s);
        if (s == null) {
          return ret;
        } else {
          return reduceSeq(f, ret, s);
        }
      }
    } else {
      Iterator<Object> chunksIterator = getChunks(defaultBundleSize).iterator();
      boolean firstFound = false;
      Object ret = null;
      while (!firstFound && chunksIterator.hasNext()) {
        IChunk chunk = (IChunk)chunksIterator.next();
        if (chunk.count() > 0) {
          firstFound = true;
          ret = chunk.nth(0);
          chunk = chunk.dropFirst();
          ret = chunk.reduce(f, ret);
        }
      }
      if (!firstFound) {
        return f.invoke();
      } else if (RT.isReduced(ret)) {
        return ((Reduced)ret).deref();
      } else {
        return reduceIterator(f, ret, chunksIterator);
      }
    }
  }

  @Override
  public Object reduce(IFn f, Object start) {
    if (isSeqSet()) {
      return reduceSeq(f, start, seq());
    } else {
      return reduceIterator(f, start, getChunks(defaultBundleSize).iterator());
    }
  }

  private Future<Object> getReduceChunkFuture(final IChunk chunk, final IFn reducef, final Object init) {
    return Agent.soloExecutor.submit(new Callable<Object>() {
        public Object call() {
          return chunk.reduce(reducef, init);
        }
      });
  }

  private Object foldSeq(IFn combinef, IFn reducef) {
    Object init = combinef.invoke();
    int n = 2 + Runtime.getRuntime().availableProcessors();
    final LinkedList<Future<Object>> futures = new LinkedList<Future<Object>>();
    IChunkedSeq s = (IChunkedSeq)RT.seq(seq());
    int k = 0;
    while (s != null && k < n) {
      futures.addLast(getReduceChunkFuture(s.chunkedFirst(), reducef, init));
      s = (IChunkedSeq)RT.seq(s.chunkedNext());
      k += 1;
    }
    Object ret = init;
    while (!futures.isEmpty()) {
      Future<Object> fut = futures.pollFirst();
      Object o = Utils.tryGetFuture(fut);
      ret = combinef.invoke(ret, o);
      if (s != null) {
        futures.addLast(getReduceChunkFuture(s.chunkedFirst(), reducef, init));
        s = (IChunkedSeq)RT.seq(s.chunkedNext());
      }
    }
    return ret;
  }

  private Object foldIterator(int n, IFn combinef, IFn reducef) {
    Object ret = combinef.invoke();
    IFn xform = getReadOptions().transduceFn;
    if (xform != null) {
      reducef = (IFn) xform.invoke(reducef);
    }
    for (Object reducedChunkValue : getReducedChunks(reducef, Utils.identity, combinef, n)) {
      ret = combinef.invoke(ret, reducedChunkValue);
    }
    return ret;
  }

  public Object fold(int n, IFn combinef, IFn reducef) {
    if (isSeqSet()) {
      return foldSeq(combinef, reducef);
    } else {
      return foldIterator(n, combinef, reducef);
    }
  }

  protected abstract Iterable<Object> getReducedChunks(IFn reduceFn, IFn completeFn, IFn initFn,
                                                       int bundleSize);

  protected abstract Options.ReadOptions getReadOptions();

  protected abstract View withOptions(Options.ReadOptions readOptions);

  public View withSampleFn(IFn sampleFn) {
    return withOptions(getReadOptions().withSampleFn(sampleFn));
  }

  public View withIndexedByFn(IFn indexedByFn) {
    return withOptions(getReadOptions().withIndexedByFn(indexedByFn));
  }

  public View withTransduceFn(IFn transduceFn) {
    return withOptions(getReadOptions().withTransduceFn(transduceFn));
  }

  private Iterable<Object> getChunks(int bundleSize) {
    IFn rf = new ChunkReduceFn(bundleSize);
    IFn xform = getReadOptions().transduceFn;
    if (xform != null) {
      rf = (IFn)xform.invoke(rf);
    }
    return getReducedChunks(rf, rf, rf, bundleSize);
  }

  private static ISeq getChunkedSeq(final Iterator<Object> chunksIterator) {
    return new LazySeq(new AFn() {
        public IChunkedSeq invoke() {
          IChunk nextChunk = null;
          while (chunksIterator.hasNext()) {
            nextChunk = (IChunk)chunksIterator.next();
            if (nextChunk.count() > 0) {
              return new ChunkedCons(nextChunk, getChunkedSeq(chunksIterator));
            }
          }
          return null;
        }
      });
  }
}
