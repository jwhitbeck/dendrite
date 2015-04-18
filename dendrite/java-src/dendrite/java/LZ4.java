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

import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4FastDecompressor;

public class LZ4 {

  private final static LZ4Factory lz4Factory = LZ4Factory.safeInstance();

  public static LZ4Compressor compressor() {
    return lz4Factory.fastCompressor();
  }

  public static LZ4FastDecompressor decompressor() {
    return lz4Factory.fastDecompressor();
  }

}
