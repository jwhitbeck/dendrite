package dendrite.java;

public class FixedLengthByteArrayPlainDecoder extends AbstractDecoder {

  private final int length;

  public FixedLengthByteArrayPlainDecoder(final ByteArrayReader baw) {
    super(baw);
    this.length = byteArrayReader.readUInt();
  }

  @Override
  public Object decode() {
    byte[] fixedLengthByteArray = new byte[length];
    byteArrayReader.readByteArray(fixedLengthByteArray, 0, length);
    return fixedLengthByteArray;
  }

}
