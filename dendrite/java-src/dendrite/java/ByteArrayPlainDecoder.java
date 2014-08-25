package dendrite.java;

public class ByteArrayPlainDecoder extends AbstractDecoder {

  public ByteArrayPlainDecoder(final ByteArrayReader baw) {
    super(baw);
  }

  @Override
  public Object decode() {
    int length = byteArrayReader.readFixedInt();
    byte[] byteArray = new byte[length];
    byteArrayReader.readByteArray(byteArray, 0, length);
    return byteArray;
  }

}
