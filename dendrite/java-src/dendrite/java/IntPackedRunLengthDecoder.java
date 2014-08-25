package dendrite.java;

import java.util.Iterator;

public class IntPackedRunLengthDecoder implements Decoder {

  private final IntFixedBitWidthPackedRunLengthDecoder int32Decoder;

  public IntPackedRunLengthDecoder(final ByteArrayReader baw) {
    ByteArrayReader byteArrayReader = baw.slice();
    int width = (int)byteArrayReader.readByte() & 0xff;
    int32Decoder = new IntFixedBitWidthPackedRunLengthDecoder(byteArrayReader, width);
  }

  @Override
  public Object decode() {
    return int32Decoder.decode();
  }

  @Override
  public int numEncodedValues() {
    return int32Decoder.numEncodedValues();
  }

  @Override
  public Iterator iterator() {
    return int32Decoder.iterator();
  }
}
