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

import java.nio.ByteBuffer;
import java.util.zip.Inflater;
import net.jpountz.lz4.LZ4FastDecompressor;

public class LZ4Decompressor implements IDecompressor {

  @Override
  public ByteBuffer decompress(final ByteBuffer byteBuffer, final int compressedLength,
                               final int decompressedLength) {
    byte[] decompressedBytes = new byte[decompressedLength];
    LZ4FastDecompressor lz4Decompressor = LZ4.decompressor();
    byte[] compressedBytes;
    int offset;
    if (byteBuffer.hasArray()) {
      compressedBytes = byteBuffer.array();
      offset = byteBuffer.arrayOffset() + byteBuffer.position();
      byteBuffer.position(byteBuffer.position() + compressedLength);
    } else {
      compressedBytes = new byte[compressedLength];
      byteBuffer.get(compressedBytes);
      offset = 0;
    }
    lz4Decompressor.decompress(compressedBytes, offset, decompressedBytes, 0, decompressedLength);
    return ByteBuffer.wrap(decompressedBytes);
  }

}
