package dendrite.java;

public interface FixedLengthByteArrayEncoder extends Finishable, ByteArrayWritable, Sizeable, Resetable {

  public void encode(byte[] bs);

}
