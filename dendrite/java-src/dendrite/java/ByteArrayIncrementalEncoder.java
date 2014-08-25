package dendrite.java;

public class ByteArrayIncrementalEncoder implements Encoder {

  private byte[] previousByteArray = null;
  private final ByteArrayDeltaLengthEncoder byteArrayEncoder;
  private final IntPackedDeltaEncoder prefixLengthsEncoder;
  private int numValues = 0;

  public ByteArrayIncrementalEncoder() {
    prefixLengthsEncoder = new IntPackedDeltaEncoder();
    byteArrayEncoder = new ByteArrayDeltaLengthEncoder();
  }

  @Override
  public void encode(final Object o) {
    final byte[] bs = (byte[]) o;
    numValues += 1;
    int firstDifferentByteIdx = 0;
    if (previousByteArray != null) {
      int i = 0;
      while (i < Math.min(bs.length, previousByteArray.length)) {
        if (bs[i] != previousByteArray[i]) {
          break;
        }
        i++;
      }
      firstDifferentByteIdx = i;
    }
    prefixLengthsEncoder.encode(firstDifferentByteIdx);
    byteArrayEncoder.encode(bs, firstDifferentByteIdx, bs.length - firstDifferentByteIdx);
    previousByteArray = bs;
  }

  @Override
  public void reset() {
    numValues = 0;
    prefixLengthsEncoder.reset();
    byteArrayEncoder.reset();
    previousByteArray = null;
  }

  @Override
  public void finish() {
    prefixLengthsEncoder.finish();
    byteArrayEncoder.finish();
  }

  @Override
  public int length() {
    return ByteArrayWriter.getNumUIntBytes(prefixLengthsEncoder.length())
      + ByteArrayWriter.getNumUIntBytes(numValues)
      + prefixLengthsEncoder.length() + byteArrayEncoder.length();
  }

  @Override
  public int estimatedLength() {
    int estimatedPrefixLengthsEncoderLength = prefixLengthsEncoder.estimatedLength();
    return ByteArrayWriter.getNumUIntBytes(estimatedPrefixLengthsEncoderLength)
      + ByteArrayWriter.getNumUIntBytes(numValues)
      + estimatedPrefixLengthsEncoderLength + byteArrayEncoder.length();
  }

  @Override
  public void flush(final ByteArrayWriter baw) {
    finish();
    baw.writeUInt(numValues);
    baw.writeUInt(prefixLengthsEncoder.length());
    prefixLengthsEncoder.flush(baw);
    byteArrayEncoder.flush(baw);
  }

  @Override
  public int numEncodedValues() {
    return numValues;
  }

}
