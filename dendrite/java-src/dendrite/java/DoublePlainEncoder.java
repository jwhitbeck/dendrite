package dendrite.java;

public class DoublePlainEncoder extends AbstractEncoder {

  @Override
  public void encode(final Object o) {
    numValues += 1;
    byteArrayWriter.writeDouble((double)o);
  }

}
