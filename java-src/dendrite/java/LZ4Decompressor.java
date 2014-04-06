package dendrite.java;

import java.util.zip.Inflater;
import net.jpountz.lz4.LZ4FastDecompressor;

public class LZ4Decompressor implements Decompressor {

  @Override
  public ByteArrayReader decompress(final ByteArrayReader bar, final int compressedSize,
                                    final int decompressedSize) {
    byte[] decompressed_bytes = new byte[decompressedSize];
    LZ4FastDecompressor lz4_decompressor = LZ4.decompressor();
    lz4_decompressor.decompress(bar.buffer, bar.position, decompressed_bytes, 0, decompressedSize);
    return new ByteArrayReader(decompressed_bytes);
  }

}
