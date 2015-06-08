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

import clojure.lang.IPersistentMap;
import clojure.lang.Obj;
import clojure.lang.Symbol;

public final class Col extends Obj {
  public final Symbol type;
  public final Symbol encoding;
  public final Symbol compression;

  public Col(Symbol type) {
    this.type = type;
    this.encoding = Types.PLAIN_SYM;
    this.compression = Types.NONE_SYM;
  }

  public Col(Symbol type, Symbol encoding) {
    this.type = type;
    this.encoding = encoding;
    this.compression = Types.NONE_SYM;
  }

  public Col(Symbol type, Symbol encoding, Symbol compression) {
    this.type = type;
    this.encoding = encoding;
    this.compression = compression;
  }

  public Col(IPersistentMap meta, Symbol type, Symbol encoding, Symbol compression) {
    super(meta);
    this.type = type;
    this.encoding = encoding;
    this.compression = compression;
  }

  @Override
  public Obj withMeta(IPersistentMap meta) {
    return new Col(meta, type, encoding, compression);
  }

  @Override
  public String toString() {
    if (encoding.equals(Types.PLAIN_SYM) && compression.equals(Types.NONE_SYM)) {
      return type.getName();
    }
    return "#col [" + type.getName() + " " + encoding.getName()
      + (compression.equals(Types.NONE_SYM)? "" : " " + compression.getName()) + "]";
  }

  @Override
  public boolean equals(Object o) {
    if ((!(o instanceof Col))) {
      return false;
    }
    Col col = (Col)o;
    return type.equals(col.type)
      && encoding.equals(col.encoding)
      && compression.equals(col.compression);
  }

  @Override
  public int hashCode() {
    return type.hashCode() ^ encoding.hashCode() ^ compression.hashCode();
  }

}
