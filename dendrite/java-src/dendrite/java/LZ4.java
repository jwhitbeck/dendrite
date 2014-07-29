package dendrite.java;

import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4FastDecompressor;

public class LZ4 {

  private final static LZ4Factory lz4_factory = LZ4Factory.safeInstance();

  public static LZ4Compressor compressor() {
    return lz4_factory.fastCompressor();
  }

  public static LZ4FastDecompressor decompressor() {
    return lz4_factory.fastDecompressor();
  }

}
