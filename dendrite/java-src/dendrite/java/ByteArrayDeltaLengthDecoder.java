package dendrite.java;

public class ByteArrayDeltaLengthDecoder extends AbstractDecoder {

  private final IntPackedDeltaDecoder lengthsDecoder;

  public ByteArrayDeltaLengthDecoder(final ByteArrayReader baw) {
    super(baw);
    int lengthsNumBytes = byteArrayReader.readUInt();
    lengthsDecoder = new IntPackedDeltaDecoder(byteArrayReader);
    byteArrayReader.position += lengthsNumBytes;
  }

  @Override
  public Object decode() {
    int length = (int)lengthsDecoder.decode();
    byte[] byteArray = new byte[length];
    byteArrayReader.readByteArray(byteArray, 0, length);
    return byteArray;
  }

  public void decodeInto(ByteArrayWriter baw) {
    int length = (int)lengthsDecoder.decode();
    byteArrayReader.readBytes(baw, length);
  }

}
