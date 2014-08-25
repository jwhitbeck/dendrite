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
