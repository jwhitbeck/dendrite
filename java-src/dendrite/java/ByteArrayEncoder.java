package dendrite.java;

public interface ByteArrayEncoder extends Finishable, ByteArrayWritable, Sizeable, Resetable {

  public void encode(byte[] bs);

}
