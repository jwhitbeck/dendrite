package dendrite.java;

public interface Decompressor {

  public ByteArrayReader decompress(ByteArrayReader bar, int compressedLength, int decompressedLength);

}
