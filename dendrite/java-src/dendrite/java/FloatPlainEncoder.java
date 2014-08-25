package dendrite.java;

public class FloatPlainEncoder extends AbstractEncoder {

  @Override
  public void encode(final Object o) {
    numValues += 1;
    byteArrayWriter.writeFloat((float)o);
  }

}
