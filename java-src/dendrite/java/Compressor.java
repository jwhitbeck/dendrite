package dendrite.java;

public interface Compressor extends Resetable {

  public int uncompressedSize();

  public int compressedSize();

  public void compressBytes(final ByteArrayWriter baw);

  public void writeCompressedTo(final ByteArrayWriter baw);

  public void writeUncompressedTo(final ByteArrayWriter baw);

}
