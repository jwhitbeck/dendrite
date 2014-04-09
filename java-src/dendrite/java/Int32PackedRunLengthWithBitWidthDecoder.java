package dendrite.java;

public class Int32PackedRunLengthWithBitWidthDecoder implements Int32Decoder {

  private final Int32PackedRunLengthDecoder int32_decoder;

  public Int32PackedRunLengthWithBitWidthDecoder(final ByteArrayReader baw) {
    int width = (int)baw.readByte() & 0xff;
    int32_decoder = new Int32PackedRunLengthDecoder(baw, width);
  }

  @Override
  public int decode() {
    return int32_decoder.decode();
  }
}
