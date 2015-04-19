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

import clojure.lang.Keyword;

import java.nio.charset.Charset;
import java.nio.ByteBuffer;

public final class Encodings {

  public final static int PLAIN = 0;
  public final static int DICTIONARY = 1;
  public final static int FREQUENCY = 2;
  public final static int VLQ = 3;
  public final static int ZIGZAG = 4;
  public final static int PACKED_RUN_LENGTH = 5;
  public final static int DELTA = 6;
  public final static int INCREMENTAL = 7;
  public final static int DELTA_LENGTH = 8;

  final static Charset utf8Charset = Charset.forName("UTF-8");

  public static byte[] stringToUFT8Bytes(String s) {
    if (s == null) {
      return null;
    }
    return s.getBytes(utf8Charset);
  }

  public static String UTF8BytesToString(byte[] bs) {
    if (bs == null) {
      return null;
    }
    return new String(bs, utf8Charset);
  }

  public static Keyword stringToKeyword(String s) {
    return Keyword.intern(s);
  }

  public static String keywordToString(Keyword k) {
    String namespace = k.sym.getNamespace();
    String name = k.sym.getName();
    if (namespace == null) {
      return name;
    }
    return namespace + "/" + name;
  }

  public static byte[] byteBufferToByteArray(ByteBuffer bb) {
    if (bb == null) {
      return null;
    }
    int length = bb.limit() - bb.position();
    byte[] bs = new byte[length];
    bb.mark();
    bb.get(bs);
    bb.reset();
    return bs;
  }

  public static ByteBuffer byteArrayToByteBuffer(byte[] bs) {
    if (bs == null) {
      return null;
    }
    return ByteBuffer.wrap(bs);
  }

}
