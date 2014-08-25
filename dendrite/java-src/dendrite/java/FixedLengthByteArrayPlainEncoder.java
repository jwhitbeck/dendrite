package dendrite.java;

public class FixedLengthByteArrayPlainEncoder extends AbstractEncoder {

  private int length = -1;

  @Override
  public void encode(final Object o) {
    final byte[] bs = (byte[]) o;
    numValues += 1;
    if (length != bs.length) {
      if (length == -1) {
        length = bs.length;
        byteArrayWriter.writeUInt(length);
      } else {
        throw new IllegalStateException("Different byte-array lengths in FixedLengthByteArrayEncoder");
      }
    }
    byteArrayWriter.writeByteArray(bs, 0, length);
  }

  @Override
  public void reset() {
    super.reset();
    length = -1;
  }

}
