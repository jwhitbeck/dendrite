package dendrite.java;

public interface Encoder extends BufferedByteArrayWriter {
  public void encode(final Object o);
  public int numEncodedValues();
}
