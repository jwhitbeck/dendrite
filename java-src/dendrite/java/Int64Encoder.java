package dendrite.java;

public interface Int64Encoder extends Finishable, ByteArrayWritable, Sizeable, Resetable {

  public void encode(long l);

}
