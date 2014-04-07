package dendrite.java;

public interface Int32Encoder extends Finishable, ByteArrayWritable, Sizeable, Resetable {

  public void encode(int i);

}
