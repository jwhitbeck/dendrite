package dendrite.java;

public class IntPlainEncoder extends AbstractEncoder {

  @Override
  public void encode(final Object o) {
    numValues += 1;
    byteArrayWriter.writeFixedInt((int) o);
  }

}
