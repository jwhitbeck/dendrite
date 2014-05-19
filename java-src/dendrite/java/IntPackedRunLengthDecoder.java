package dendrite.java;

public class IntPackedRunLengthDecoder implements IntDecoder {

  private final IntFixedBitWidthPackedRunLengthDecoder int32_decoder;

  public IntPackedRunLengthDecoder(final ByteArrayReader baw) {
    int width = (int)baw.readByte() & 0xff;
    int32_decoder = new IntFixedBitWidthPackedRunLengthDecoder(baw, width);
  }

  @Override
  public int decode() {
    return int32_decoder.decode();
  }
}
