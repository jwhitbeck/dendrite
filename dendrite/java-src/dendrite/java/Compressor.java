package dendrite.java;

public interface Compressor extends Resetable, Flushable {

  public int uncompressedLength();

  public int compressedLength();

  public void compress(final Flushable byteArrayWritable);

}
