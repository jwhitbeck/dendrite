package dendrite.java;

import java.util.Iterator;

public class IntPackedRunLengthDecoder implements Decoder {

  private final IntFixedBitWidthPackedRunLengthDecoder int32_decoder;

  public IntPackedRunLengthDecoder(final ByteArrayReader baw) {
    ByteArrayReader byte_array_reader = baw.slice();
    int width = (int)byte_array_reader.readByte() & 0xff;
    int32_decoder = new IntFixedBitWidthPackedRunLengthDecoder(byte_array_reader, width);
  }

  @Override
  public Object decode() {
    return int32_decoder.decode();
  }

  @Override
  public int numEncodedValues() {
    return int32_decoder.numEncodedValues();
  }

  @Override
  public Iterator iterator() {
    return int32_decoder.iterator();
  }
}
