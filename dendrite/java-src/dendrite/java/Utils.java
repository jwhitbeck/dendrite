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
import clojure.lang.Cons;
import clojure.lang.IFn;
import clojure.lang.ISeq;
import clojure.lang.LazySeq;
import clojure.lang.RT;

import java.io.IOException;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
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

  public static IFn and(final IFn fna, final IFn fnb) {
    return new AFn() {
      public Object invoke(Object o) {
        return RT.booleanCast(fna.invoke(o)) && RT.booleanCast(fnb.invoke(o));
      }
    };
  }

  public static <T> T tryGetFuture(Future<T> fut) {
    try {
      return fut.get();
    } catch (Exception e) {
      throw new IllegalStateException(e);
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

  public static ByteBuffer mapFileChannel(FileChannel fileChannel, long offset, long length)
    throws IOException {
    return fileChannel.map(FileChannel.MapMode.READ_ONLY, offset, length);
  }
}
