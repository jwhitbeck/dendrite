package dendrite.java;

public interface DoubleEncoder extends Finishable, ByteArrayWritable, Sizeable, Resetable {

  public void encode(double d);

}
