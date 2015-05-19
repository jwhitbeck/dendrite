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
import clojure.lang.ASeq;
import clojure.lang.Agent;
import clojure.lang.Cons;
import clojure.lang.IFn;
import clojure.lang.IPersistentCollection;
import clojure.lang.IPersistentMap;
import clojure.lang.ISeq;
import clojure.lang.LazySeq;
import clojure.lang.RT;

import java.io.IOException;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.LinkedList;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

public final class Utils {

  public static IFn comp(final IFn f, final IFn g) {
    return new AFn() {
      public Object invoke(Object o) {
        return f.invoke(g.invoke(o));
      }
    };
  }

  public static IFn comp(final IFn f, final IFn g, final IFn h) {
    return new AFn() {
      public Object invoke(Object o) {
        return f.invoke(g.invoke(h.invoke(o)));
      }
    };
  }

  public static IFn comp(final IFn... fs) {
    if (fs.length == 0) {
      return null;
    } else if (fs.length == 1) {
      return fs[0];
    } else if (fs.length == 2) {
      return comp(fs[0], fs[1]);
    } else if (fs.length == 3) {
      return comp(fs[0], fs[1], fs[2]);
    } else {
      return new AFn() {
        public Object invoke(Object o) {
          Object ret = o;
          for (int i=0; i<fs.length; ++i) {
            ret = fs[i].invoke(ret);
          }
          return ret;
        }
      };
    }
  }

  private final static class Concat extends ASeq {

    private final ISeq head;
    private final ISeq tail;

    private Concat(ISeq head, ISeq tail) {
      this.head = head;
      this.tail = tail;
    }

    private Concat(IPersistentMap meta, ISeq head, ISeq tail) {
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
        return RT.seq(tail);
      }
      return new Concat(next, tail);
    }
  }

  public static ISeq concat(Object head, Object tail) {
    if (RT.seq(head) == null) {
      return RT.seq(tail);
    }
    return new Concat(RT.seq(head), RT.seq(tail));
  }

  private static Future getPmapFuture(final IFn fn, final Object o) {
    return Agent.soloExecutor.submit(new Callable<Object>() {
        public Object call() {
          return fn.invoke(o);
        }
      });
  }

  private static Object tryGet(Future fut) {
    try {
      return fut.get();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static ISeq pmapStep(final IFn fn, final LinkedList<Future> futures, final ISeq s) {
    return new LazySeq(new AFn() {
        public Object invoke() {
          if (futures.size() > 0) {
            Future fut = futures.pollFirst();
            if (RT.seq(s) != null) {
              futures.addLast(getPmapFuture(fn, s.first()));
              return new Cons(tryGet(fut), pmapStep(fn, futures, s.next()));
            } else {
              return new Cons(tryGet(fut), pmapStep(fn, futures, null));
            }
          }
          return null;
        }
      });
  }

  public static ISeq pmap(IFn fn, ISeq s) {
    int n = 2 + Runtime.getRuntime().availableProcessors();
    final LinkedList<Future> futures = new LinkedList<Future>();
    ISeq rest = RT.seq(s);
    int i = 0;
    while (rest != null && i<n) {
      futures.addLast(getPmapFuture(fn, rest.first()));
      rest = rest.next();
      i += 1;
    }
    return pmapStep(fn, futures, rest);
  }

  public static void doAll(ISeq s) {
    if (RT.seq(s) != null) {
      doAll(s.next());
    }
  }

  public static ISeq map(final IFn fn, final ISeq s) {
    return new LazySeq(new AFn() {
        public Object invoke() {
          if (RT.seq(s) == null) {
            return null;
          } else {
            return new Cons(fn.invoke(s.first()), map(fn, s.next()));
          }
        }
      });
  }

  public static FileChannel getReadingFileChannel(File file) throws IOException {
    return FileChannel.open(file.toPath(), StandardOpenOption.READ);
  }

  public static FileChannel getWritingFileChannel(File file) throws IOException {
    return FileChannel.open(file.toPath(), StandardOpenOption.WRITE, StandardOpenOption.CREATE,
                            StandardOpenOption.TRUNCATE_EXISTING);
  }

  public static ByteBuffer mapFileChannel(FileChannel fileChannel, int offset, int length) throws IOException {
    return fileChannel.map(FileChannel.MapMode.READ_ONLY, offset, length);
  }

}
