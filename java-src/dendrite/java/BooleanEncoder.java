package dendrite.java;

public interface BooleanEncoder extends Finishable, ByteArrayWritable, Sizeable, Resetable {

  public void encode(boolean b);

}
