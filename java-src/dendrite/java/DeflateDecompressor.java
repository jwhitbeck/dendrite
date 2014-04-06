package dendrite.java;

import java.util.zip.Inflater;
import java.util.zip.DataFormatException;

public class DeflateDecompressor implements Decompressor {

  @Override
  public ByteArrayReader decompress(final ByteArrayReader bar, final int compressedSize,
                                    final int decompressedSize) {
    Inflater inflater = new Inflater(true);
    inflater.setInput(bar.buffer, bar.position, compressedSize);
    byte[] decompressed_bytes = new byte[decompressedSize];
    try {
      inflater.inflate(decompressed_bytes);
    } catch (DataFormatException dfe) {
      throw new IllegalStateException(dfe);
    } finally {
      inflater.end();
    }
    return new ByteArrayReader(decompressed_bytes);
  }

}
