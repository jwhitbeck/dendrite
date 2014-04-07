package dendrite.java;

public interface FloatEncoder extends Finishable, ByteArrayWritable, Sizeable, Resetable {

  public void encode(float f);

}
