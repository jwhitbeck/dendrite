package dendrite.java;

import java.util.zip.Inflater;
import net.jpountz.lz4.LZ4FastDecompressor;

public class LZ4Decompressor implements Decompressor {

  @Override
  public ByteArrayReader decompress(final ByteArrayReader bar, final int compressedLength,
                                    final int decompressedLength) {
    byte[] decompressedBytes = new byte[decompressedLength];
    LZ4FastDecompressor lz4Decompressor = LZ4.decompressor();
    lz4Decompressor.decompress(bar.buffer, bar.position, decompressedBytes, 0, decompressedLength);
    return new ByteArrayReader(decompressedBytes);
  }

}
