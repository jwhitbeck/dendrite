package dendrite.java;

public interface BufferedByteArrayWriter extends Lengthable, Resetable, Finishable, ByteArrayWritable {
  public int estimatedLength();
}
