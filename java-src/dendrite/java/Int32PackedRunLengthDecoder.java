package dendrite.java;

public class Int32PackedRunLengthDecoder implements Int32Decoder {

  private final Int32FixedBitWidthPackedRunLengthDecoder int32_decoder;

  public Int32PackedRunLengthDecoder(final ByteArrayReader baw) {
    int width = (int)baw.readByte() & 0xff;
    int32_decoder = new Int32FixedBitWidthPackedRunLengthDecoder(baw, width);
  }

  @Override
  public int decode() {
    return int32_decoder.decode();
  }
}
