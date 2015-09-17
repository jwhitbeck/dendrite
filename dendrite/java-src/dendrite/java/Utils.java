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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.Future;

public final class Utils {

  public static final IFn identity = new AFn() {
      public Object invoke(Object o) {
        return o;
      }
    };

  public static IFn comp(final IFn g, final IFn f) {
    return new AFn() {
      public Object invoke(Object o) {
        return f.invoke(g.invoke(o));
      }
    };
  }

  public static IFn comp(final IFn h, final IFn g, final IFn f) {
    return new AFn() {
      public Object invoke(Object o) {
        return f.invoke(g.invoke(h.invoke(o)));
      }
    };
  }

  public static IFn comp(List<IFn> fs) {
    switch (fs.size()) {
    case 0: return null;
    case 1: return fs.get(0);
    case 2: return comp(fs.get(0), fs.get(1));
    case 3: return comp(fs.get(0), fs.get(1), fs.get(2));
    default: return compLoop(fs);
    }
  }

  private static IFn compLoop(final List<IFn> fs) {
    return new AFn() {
      public Object invoke(Object o) {
        Object ret = o;
        for (IFn f : fs) {
          ret = f.invoke(ret);
        }
        return ret;
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
