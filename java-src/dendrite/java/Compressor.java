package dendrite.java;

public interface Compressor extends Resetable, ByteArrayWritable {

  public int uncompressedLength();

  public int compressedLength();

  public void compress(final ByteArrayWritable byteArrayWritable);

}
