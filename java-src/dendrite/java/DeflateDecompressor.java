package dendrite.java;

import java.util.zip.Inflater;
import java.util.zip.DataFormatException;

public class DeflateDecompressor implements Decompressor {

  @Override
  public ByteArrayReader decompress(final ByteArrayReader bar, final int compressedLength,
                                    final int decompressedLength) {
    Inflater inflater = new Inflater(true);
    inflater.setInput(bar.buffer, bar.position, compressedLength);
    byte[] decompressed_bytes = new byte[decompressedLength];
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
