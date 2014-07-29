package dendrite.java;

public class LongPlainDecoder implements LongDecoder {

  private final ByteArrayReader byte_array_reader;

  public LongPlainDecoder(final ByteArrayReader baw) {
    byte_array_reader = baw;
  }

  @Override
  public long decode() {
    return byte_array_reader.readFixedLong();
  }

}
