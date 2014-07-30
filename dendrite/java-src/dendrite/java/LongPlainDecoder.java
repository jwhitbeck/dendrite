package dendrite.java;

public class LongPlainDecoder extends AbstractDecoder {

  public LongPlainDecoder(final ByteArrayReader baw) {
    super(baw);
  }

  @Override
  public Object decode() {
    return byte_array_reader.readFixedLong();
  }

}
