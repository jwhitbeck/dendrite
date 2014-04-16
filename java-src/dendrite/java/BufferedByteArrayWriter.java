package dendrite.java;

public interface BufferedByteArrayWriter extends Sizeable, Resetable, Finishable, ByteArrayWritable {
  public int estimatedSize();
}
