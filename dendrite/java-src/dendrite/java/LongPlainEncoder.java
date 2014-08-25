package dendrite.java;

public class LongPlainEncoder extends AbstractEncoder {

  @Override
  public void encode(final Object o) {
    numValues += 1;
    byteArrayWriter.writeFixedLong((long)o);
  }

}
