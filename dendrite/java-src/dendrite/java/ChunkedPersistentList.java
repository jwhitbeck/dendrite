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

import clojure.lang.ASeq;
import clojure.lang.ArrayChunk;
import clojure.lang.Counted;
import clojure.lang.IChunk;
import clojure.lang.IChunkedSeq;
import clojure.lang.IHashEq;
import clojure.lang.IPersistentCollection;
import clojure.lang.IPersistentList;
import clojure.lang.IPersistentMap;
import clojure.lang.IPersistentStack;
import clojure.lang.ITransientCollection;
import clojure.lang.ISeq;
import clojure.lang.PersistentList;
import clojure.lang.RT;

import java.io.Serializable;

public final class ChunkedPersistentList extends ASeq implements IChunkedSeq, IPersistentList {

  private final Node head;
  private final int cnt;
  private final int offset;

  static ChunkedPersistentList create(Node head, int cnt, int offset) {
    if (cnt == 0) {
      return null;
    }
    return new ChunkedPersistentList(head, cnt, offset);
  }

  ChunkedPersistentList(Node head, int cnt, int offset) {
    this.head = head;
    this.cnt = cnt;
    this.offset = offset;
  }

  ChunkedPersistentList(IPersistentMap meta, Node head, int cnt, int offset) {
    super(meta);
    this.head = head;
    this.cnt = cnt;
    this.offset = offset;
  }

  @Override
  public int count() {
    return cnt;
  }

  @Override
  public IChunk chunkedFirst() {
    return new ArrayChunk(head.array, offset, head.cnt);
  }

  @Override
  public ChunkedPersistentList chunkedNext() {
    if (head.next != null && cnt > (head.cnt - offset)) {
      return new ChunkedPersistentList(head.next, cnt - head.cnt + offset, 0);
    }
    return null;
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
  public ChunkedPersistentList withMeta(IPersistentMap meta){
    return new ChunkedPersistentList(meta, head, cnt, offset);
  }

  @Override
  public Object first(){
    return head.array[offset];
  }

  @Override
  public ChunkedPersistentList next(){
    if (cnt > 1) {
      if(offset + 1 < head.cnt) {
        return new ChunkedPersistentList(head, cnt-1, offset+1);
      } else {
        return chunkedNext();
      }
    }
    return null;
  }

  @Override
  public ChunkedPersistentList pop() {
    return next();
  }

  @Override
  public Object peek() {
    return head.array[offset];
  }

  public ChunkedPersistentList take(int n) {
    if (n > 0) {
      return new ChunkedPersistentList(head, n, offset);
    }
    return null;
  }

  public ChunkedPersistentList drop(int n) {
    Node node = head;
    int off = offset;
    int remaining = n;
    while (node != null && remaining > 0) {
      int numValues = node.cnt - off;
      if (numValues <= remaining) {
        node = node.next;
        remaining -= numValues;
        off = 0;
      } else {
        off = remaining;
        remaining = 0;
      }
    }
    if (node == null) {
      return null;
    }
    return new ChunkedPersistentList(node, cnt - n, off);
  }

  private final static class Node implements Serializable {
    Node next = null;
    int cnt = 0;
    final Object[] array = new Object[32];

    Node add(Object o) {
      array[cnt] = o;
      cnt += 1;
      return this;
    }
  }

  public static ITransientCollection newEmptyTransient() {
    return new TransientChunkedList();
  }

  private static class TransientChunkedList implements ITransientCollection {

    private Node head = null;
    private Node tail = null;
    private int cnt = 0;

    @Override
    public ITransientCollection conj(Object val) {
      if (tail == null) {
        Node newTail = new Node().add(val);
        head = newTail;
        tail = newTail;
      } else if (tail.cnt < 32) {
        tail.add(val);
      } else {
        Node newTail = new Node().add(val);
        tail.next = newTail;
        tail = newTail;
      }
      cnt += 1;
      return this;
    }

    @Override
    public IPersistentCollection persistent() {
      return create(head, cnt, 0);
    }
  }
}
