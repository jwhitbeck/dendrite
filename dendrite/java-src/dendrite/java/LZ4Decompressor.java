package dendrite.java;

import java.util.zip.Inflater;
import net.jpountz.lz4.LZ4FastDecompressor;

public class LZ4Decompressor implements Decompressor {

  @Override
  public ByteArrayReader decompress(final ByteArrayReader bar, final int compressedLength,
                                    final int decompressedLength) {
    byte[] decompressed_bytes = new byte[decompressedLength];
    LZ4FastDecompressor lz4_decompressor = LZ4.decompressor();
    lz4_decompressor.decompress(bar.buffer, bar.position, decompressed_bytes, 0, decompressedLength);
    return new ByteArrayReader(decompressed_bytes);
  }

}
