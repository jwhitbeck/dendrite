package dendrite.java;

public class IntPlainDecoder extends AbstractDecoder {

  public IntPlainDecoder(final ByteArrayReader baw) {
    super(baw);
  }

  @Override
  public Object decode() {
    return byteArrayReader.readFixedInt();
  }

}
