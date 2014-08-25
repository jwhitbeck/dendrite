package dendrite.java;

public class LongPlainDecoder extends AbstractDecoder {

  public LongPlainDecoder(final ByteArrayReader baw) {
    super(baw);
  }

  @Override
  public Object decode() {
    return byteArrayReader.readFixedLong();
  }

}
