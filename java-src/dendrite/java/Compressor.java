package dendrite.java;

public interface Compressor extends Resetable, ByteArrayWritable {

  public int uncompressedSize();

  public int compressedSize();

  public void compress(final ByteArrayWritable byteArrayWritable);

}
