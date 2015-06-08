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

import clojure.lang.Symbol;

import java.nio.ByteBuffer;

public final class CustomType implements IWriteable {

  public final int type;
  public final int baseType;
  public final Symbol sym;

  public CustomType(int type, int baseType, Symbol sym) {
    this.type = type;
    this.baseType = baseType;
    this.sym = sym;
  }

  @Override
  public void writeTo(MemoryOutputStream mos) {
    Bytes.writeUInt(mos, type);
    Bytes.writeUInt(mos, baseType);
    Bytes.writeByteArray(mos, Types.toByteArray(Types.toString(sym)));
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof CustomType)) {
      return false;
    }
    CustomType ct = (CustomType) o;
    return type == ct.type
      && baseType == ct.baseType
      && sym.equals(ct.sym);
  }

  @Override
  public int hashCode() {
    throw new UnsupportedOperationException();
  }

  public static CustomType read(ByteBuffer bb) {
    return new CustomType(Bytes.readUInt(bb),
                          Bytes.readUInt(bb),
                          Types.toSymbol(Types.toString(Bytes.readByteArray(bb))));
  }

}
