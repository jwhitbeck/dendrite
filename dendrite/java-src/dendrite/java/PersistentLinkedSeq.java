package dendrite.java;

import clojure.lang.ASeq;
import clojure.lang.ArrayChunk;
import clojure.lang.Counted;
import clojure.lang.Cons;
import clojure.lang.IChunk;
import clojure.lang.IChunkedSeq;
import clojure.lang.IHashEq;
import clojure.lang.IPersistentCollection;
import clojure.lang.IPersistentMap;
import clojure.lang.ITransientCollection;
import clojure.lang.ISeq;
import clojure.lang.PersistentList;
import clojure.lang.Util;

import java.util.List;
import java.util.Iterator;
import java.io.Serializable;

public final class PersistentLinkedSeq extends ASeq implements IChunkedSeq {

  private final Node head;
  private final int cnt;
  private final int idx;
  private final int offset;

  static PersistentLinkedSeq create(final Node h, final int c, final int i, final int o) {
    if (c == 0) {
      return null;
    }
    return new PersistentLinkedSeq(h, c, i, o);
  }

  PersistentLinkedSeq(final Node h, final int c, final int i, final int o) {
    head = h;
    cnt = c;
    idx = i;
    offset = o;
  }

  PersistentLinkedSeq(final IPersistentMap meta, final Node h, final int c, final int i, final int o) {
    super(meta);
    head = h;
    cnt = c;
    idx = i;
    offset = o;
  }

  @Override
  public int count() {
    return cnt - (32 * idx + offset);
  }


  @Override
  public IChunk chunkedFirst() {
    return new ArrayChunk(head.array, offset, head.cnt);
  }

  @Override
  public ISeq chunkedNext() {
    if (head.next != null) {
      return new PersistentLinkedSeq(head.next, cnt, idx+1, 0);
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
  public PersistentLinkedSeq withMeta(IPersistentMap meta){
    return new PersistentLinkedSeq(meta, head, cnt, idx, offset);
  }

  @Override
  public Object first(){
    return head.array[offset];
  }

  @Override
  public ISeq next(){
    if(offset + 1 < head.cnt)
      return new PersistentLinkedSeq(head, cnt, idx, offset+1);
    return chunkedNext();
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

  public static TransientLinkedSeq newEmptyTransient() {
    return new TransientLinkedSeq();
  }

  public static class TransientLinkedSeq implements ITransientCollection {

    private Node head = null;
    private Node tail = null;
    private int cnt = 0;

    @Override
    public ITransientCollection conj(Object val) {
      if (tail == null) {
        Node new_tail = new Node().add(val);
        head = new_tail;
        tail = new_tail;
      } else if (tail.cnt < 32) {
        tail.add(val);
      } else {
        Node new_tail = new Node().add(val);
        tail.next = new_tail;
        tail = new_tail;
      }
      cnt += 1;
      return this;
    }

    @Override
    public IPersistentCollection persistent() {
      return create(head, cnt, 0, 0);
    }
  }
}
