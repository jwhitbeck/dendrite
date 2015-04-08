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

import clojure.lang.ArrayChunk;
import clojure.lang.ASeq;
import clojure.lang.Cons;
import clojure.lang.Counted;
import clojure.lang.IChunk;
import clojure.lang.IChunkedSeq;
import clojure.lang.IFn;
import clojure.lang.IPersistentCollection;
import clojure.lang.IPersistentMap;
import clojure.lang.ISeq;
import clojure.lang.ITransientCollection;
import clojure.lang.Obj;
import clojure.lang.PersistentList;
import clojure.lang.RT;
import clojure.lang.Seqable;
import clojure.lang.Sequential;

import java.util.List;

public final class StripedRecordBundleSeq extends Obj implements ISeq, Seqable, Sequential {

  private StripedRecordBundleGenerator generator;
  private ISeq s;

  public static StripedRecordBundleSeq create(final int partitionSize,
                                              final IPersistentCollection recordGroups) {
    return new StripedRecordBundleSeq(new StripedRecordBundleGenerator(RT.seq(recordGroups), partitionSize));
  }

  private StripedRecordBundleSeq(final StripedRecordBundleGenerator generator) {
    this.generator = generator;
  }

  private StripedRecordBundleSeq(final IPersistentMap meta, final StripedRecordBundleGenerator generator,
                                 final ISeq s) {
    super(meta);
    this.generator = generator;
    this.s = s;
  }

  @Override
  public synchronized ISeq seq() {
    if (generator != null) {
      Object bundle = generator.getNext();
      if (bundle == null) {
        s = null;
      } else {
        s = new Cons(bundle, new StripedRecordBundleSeq(generator));
      }
      generator = null;
    }
    return s;
  }

  @Override
  public Object first() {
    seq();
    if (s == null) {
      return null;
    }
    return s.first();
  }

  @Override
  public ISeq next() {
    seq();
    if (s == null) {
      return null;
    }
    return s.next();
  }

  @Override
  public ISeq more() {
    seq();
    if (s == null) {
      return PersistentList.EMPTY;
    }
    return s.more();
  }

  @Override
  public ISeq cons(final Object o) {
    return new Cons(o, this);
  }

  @Override
  public boolean equiv(final Object o){
    ISeq s = seq();
    if(s != null) {
      return s.equiv(o);
    } else {
      return (o instanceof Sequential || o instanceof List) && RT.seq(o) == null;
    }
  }

  @Override
  public IPersistentCollection empty(){
    return PersistentList.EMPTY;
  }

  @Override
  public int count() {
    seq();
    if (s == null) {
      return 0;
    }
    return s.count();
  }

  @Override
  public Obj withMeta(final IPersistentMap meta) {
    return new StripedRecordBundleSeq(meta, generator, s);
  }

  private static class StripedRecordBundleGenerator {

    private ISeq recordGroups;
    private final ISeq[] columnChunks;
    private final IChunkedSeq[] pages;
    private final IChunk[] carriedChunks;
    private final int partitionSize;

    private StripedRecordBundleGenerator(final ISeq recordGroups, final int partitionSize) {
      int numCols = (RT.seq(recordGroups.first())).count();
      this.recordGroups = recordGroups;
      this.columnChunks = new ISeq[numCols];
      this.pages = new IChunkedSeq[numCols];
      this.carriedChunks = new IChunk[numCols];
      this.partitionSize = partitionSize;
    }

    private StripedRecordBundleGenerator(final ISeq recordGroups, final ISeq[] columnChunks,
                                         final IChunkedSeq[] pages, final IChunk[] carriedChunks,
                                         final int partitionSize) {
      this.recordGroups = recordGroups;
      this.columnChunks = columnChunks;
      this.pages = pages;
      this.carriedChunks = carriedChunks;
      this.partitionSize = partitionSize;
    }

    private StripedRecordBundle getNext() {
      ISeq firstChunks = takeChunked(0, partitionSize);
      if (firstChunks == null) {
        if (recordGroups == null) {
          return null;
        }
        getNextRecordGroup();
        firstChunks = takeChunked(0, partitionSize);
      }
      int n = Math.min(firstChunks.count(), partitionSize);
      ISeq[] columnValuesSeq = new ISeq[columnChunks.length];
      columnValuesSeq[0] = firstChunks;
      for (int i=1; i<columnValuesSeq.length; ++i) {
        columnValuesSeq[i] = takeChunked(i, n);
        // TODO add count check here??
      }
      return new StripedRecordBundle(columnValuesSeq, n);
    }

    private void getNextRecordGroup() {
      ISeq columnChunksSeq = RT.seq(recordGroups.first());
      int i = 0;
      ISeq columnChunk = null;
      do {
        columnChunk = RT.seq(columnChunksSeq.first());
        columnChunks[i] = columnChunk;
        columnChunksSeq = columnChunksSeq.next();
        i++;
      } while (columnChunksSeq != null);
      recordGroups = recordGroups.next();
    }

    private IChunkedSeq getPage(final int colIdx) {
      IChunkedSeq page = pages[colIdx];
      if (page != null) {
        return page;
      }
      ISeq columnChunk = columnChunks[colIdx];
      if (columnChunk != null) {
        columnChunks[colIdx]  = columnChunk.next();
        return (IChunkedSeq) columnChunk.first();
      }
      return null;
    }

    private IChunk getNextPageChunk(final int colIdx) {
      IChunkedSeq page = getPage(colIdx);
      if (page != null) {
        pages[colIdx] = (IChunkedSeq) page.chunkedNext();
        return page.chunkedFirst();
      }
      return null;
    }

    private IChunk getNextCarriedChunk(final int colIdx) {
      IChunk carried = carriedChunks[colIdx];
      if (carried != null) {
        carriedChunks[colIdx] = null;
        return carried;
      }
      return null;
    }

    private ISeq takeChunked(final int colIdx, final int n) {
      IChunk chunk = getNextCarriedChunk(colIdx);
      chunk = (chunk == null)? getNextPageChunk(colIdx) : chunk;
      if (chunk == null) {
        return null;
      }
      int c = chunk.count();
      ChunkedSeq headSeq = new ChunkedSeq(chunk);
      ChunkedSeq tailSeq = headSeq;
      while (c < n) {
        chunk = getNextPageChunk(colIdx);
        if (chunk == null) {
          break;
        }
        ChunkedSeq cs = new ChunkedSeq(chunk);
        tailSeq.tail = cs;
        tailSeq = cs;
        c += chunk.count();
      }
      if (chunk != null && (c-n) > 0){
        carriedChunks[colIdx] = butlast(chunk, c-n);
      }
      return headSeq;
    }

    private static IChunk butlast(final IChunk chunk, final int n) {
      int offset = chunk.count() - n;
      Object[] array = new Object[n];
      for (int i=0; i<n; i++) {
        array[i] = chunk.nth(offset+i);
      }
      return new ArrayChunk(array);
    }

  }

  private static class ChunkedSeq extends ASeq implements IChunkedSeq {
    private final IChunk head;
    private ChunkedSeq tail;

    ChunkedSeq(final IChunk head) {
      this.head = head;
      this.tail = null;
    }

    ChunkedSeq(final IChunk head, final ChunkedSeq tail) {
      this.head = head;
      this.tail = tail;
    }

    ChunkedSeq(final IPersistentMap meta, final IChunk head, final ChunkedSeq tail) {
      super(meta);
      this.head = head;
      this.tail = tail;
    }

    @Override
    public int count() {
      return head.count() + ((tail == null)? 0 : tail.count()) ;
    }

    @Override
    public Object first() {
      return head.nth(0);
    }

    @Override
    public ISeq next() {
      if (head.count() > 1) {
        return new ChunkedSeq(head.dropFirst(), tail);
      } else if (tail != null) {
        return new ChunkedSeq(tail.chunkedFirst(), tail.chunkedNext());
      }
      return null;
    }

    @Override
    public IChunk chunkedFirst() {
      return head;
    }

    @Override
    public ChunkedSeq chunkedNext() {
      return tail;
    }

    @Override
    public ISeq chunkedMore() {
      return tail;
    }

    @Override
    public ChunkedSeq withMeta(final IPersistentMap meta) {
      return new ChunkedSeq(meta, head, tail);
    }

  }
}
