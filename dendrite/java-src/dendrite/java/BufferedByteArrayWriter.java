package dendrite.java;

public interface BufferedByteArrayWriter extends Lengthable, Resetable, Finishable, Flushable {
  public int estimatedLength();
}
