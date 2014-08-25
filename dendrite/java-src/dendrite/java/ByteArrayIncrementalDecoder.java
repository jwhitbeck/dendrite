package dendrite.java;

public class ByteArrayIncrementalDecoder extends AbstractDecoder {

  private final ByteArrayDeltaLengthDecoder byteArrayDecoder;
  private final IntPackedDeltaDecoder prefixLengthsDecoder;
  private final ByteArrayWriter buffer;

  public ByteArrayIncrementalDecoder(final ByteArrayReader baw) {
    super(baw);
    int prefixLengthsNumBytes = byteArrayReader.readUInt();
    prefixLengthsDecoder = new IntPackedDeltaDecoder(byteArrayReader);
    byteArrayDecoder = new ByteArrayDeltaLengthDecoder(byteArrayReader.sliceAhead(prefixLengthsNumBytes));
    buffer = new ByteArrayWriter(128);
  }

  @Override
  public Object decode() {
    int prefixLength = (int)prefixLengthsDecoder.decode();
    buffer.position = prefixLength;
    byteArrayDecoder.decodeInto(buffer);
    byte[] byteArray = new byte[buffer.length()];
    System.arraycopy(buffer.buffer, 0, byteArray, 0, buffer.length());
    return byteArray;
  }

}
