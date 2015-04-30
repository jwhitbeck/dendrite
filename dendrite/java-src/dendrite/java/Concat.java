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
import clojure.lang.IPersistentMap;
import clojure.lang.ISeq;
import clojure.lang.RT;

public final class Concat extends ASeq {

  private final ISeq head;
  private final ISeq tail;

  public Concat(ISeq head, ISeq tail) {
    this.head = head;
    this.tail = tail;
  }

  public Concat(IPersistentMap meta, ISeq head, ISeq tail) {
    super(meta);
    this.head = head;
    this.tail = tail;
  }

  @Override
  public Concat withMeta(IPersistentMap meta){
    return new Concat(meta, head, tail);
  }

  @Override
  public int count() {
    return RT.count(head) + RT.count(tail);
  }

  @Override
  public Object first() {
    return head.first();
  }

  @Override
  public ISeq next() {
    ISeq next = head.next();
    if (next == null) {
      return tail;
    }
    return new Concat(next, tail);
  }
}
