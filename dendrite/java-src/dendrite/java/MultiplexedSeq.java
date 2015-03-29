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
import clojure.lang.ArraySeq;
import clojure.lang.ASeq;
import clojure.lang.Obj;
import clojure.lang.IChunkedSeq;
import clojure.lang.IChunk;
import clojure.lang.IChunkedSeq;
import clojure.lang.IPersistentMap;
import clojure.lang.ISeq;
import clojure.lang.PersistentList;
import clojure.lang.RT;
import clojure.lang.Seqable;

import java.util.List;

public final class MultiplexedSeq extends ASeq implements IChunkedSeq {

  private final ISeq[] seqs;
  private final ArraySeq[] arraySeqs;
  private final int offset;
  private final int end;

  private MultiplexedSeq(final ISeq[] seqs, final ArraySeq[] arraySeqs, final int offset, final int end) {
    this.seqs = seqs;
    this.arraySeqs = arraySeqs;
    this.offset = offset;
    this.end = end;
  }

  private MultiplexedSeq(final IPersistentMap meta, final ISeq[] seqs,
                         final ArraySeq[] arraySeqs, final int offset, final int end) {
    super(meta);
    this.seqs = seqs;
    this.arraySeqs = arraySeqs;
    this.offset = offset;
    this.end = end;
  }

  public static MultiplexedSeq create(final List seqables) {
    int n = seqables.size();
    ISeq[] seqs = new ISeq[n];
    int i = 0;
    for (Object seqable : seqables) {
      ISeq s = RT.seq(seqable);
      if (s == null) {
        return null;
      }
      seqs[i] = s;
      i += 1;
    }
    ArraySeq[] arraySeqs = new ArraySeq[32];
    int end = step(seqs, arraySeqs);
    if (end == 0) {
      return null;
    }
    return new MultiplexedSeq(seqs, arraySeqs, 0, end);
  }

  private static int step(final ISeq[] seqs, final ArraySeq[] arraySeqs) {
    int m = arraySeqs.length;
    int n = seqs.length;
    int end = 0;
    for(int j=0; j<m; ++j) {
      Object[] row = new Object[n];
      for (int i=0; i<n; ++i) {
        ISeq s = seqs[i];
        if ((s == null) || (s.seq() == null)) {
          return end;
        }
        row[i] = s.first();
        seqs[i] = s.next();
      }
      arraySeqs[j] = ArraySeq.create(row);
      end += 1;
    }
    return end;
  }

  @Override
  public IChunk chunkedFirst() {
    return new ArrayChunk(arraySeqs, offset, end);
  }

  @Override
  public ISeq chunkedNext() {
    if (end < 32) {
      return null;
    }
    ISeq[] nextSeqs = new ISeq[seqs.length];
    System.arraycopy(seqs, 0, nextSeqs, 0, seqs.length);
    ArraySeq[] nextArraySeqs = new ArraySeq[32];
    int nextEnd = step(nextSeqs, nextArraySeqs);
    if (nextEnd == 0) {
      return null;
    }
    return new MultiplexedSeq(nextSeqs, nextArraySeqs, 0, nextEnd);
  }

  @Override
  public ISeq chunkedMore() {
    ISeq s = chunkedNext();
    if (s == null) {
      return PersistentList.EMPTY;
    }
    return s;
  }

  @Override
  public Object first() {
    return arraySeqs[offset];
  }

  @Override
  public ISeq next() {
    if (offset == end - 1) {
      if (end < 32) {
        return null;
      }
      ISeq[] nextSeqs = new ISeq[seqs.length];
      System.arraycopy(seqs, 0, nextSeqs, 0, seqs.length);
      ArraySeq[] nextArraySeqs = new ArraySeq[32];
      int nextEnd = step(nextSeqs, nextArraySeqs);
      if (nextEnd == 0) {
        return null;
      }
      return new MultiplexedSeq(nextSeqs, nextArraySeqs, 0, nextEnd);
    } else {
      return new MultiplexedSeq(seqs, arraySeqs, offset + 1, end);
    }
  }

  @Override
  public Obj withMeta(final IPersistentMap meta) {
    return new MultiplexedSeq(meta, seqs, arraySeqs, offset, end);
  }


}
