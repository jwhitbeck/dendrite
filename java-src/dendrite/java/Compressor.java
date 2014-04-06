package dendrite.java;

public interface Compressor extends Resetable {

  public int uncompressedSize();

  public int compressedSize();

  public void compress(final byte[] bs, final int offset, final int length, final ByteArrayWriter baw);

}
