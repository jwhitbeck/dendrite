package dendrite.java;

public interface FixedLengthByteArrayEncoder extends BufferedByteArrayWriter {

  public void encode(byte[] bs);

}
