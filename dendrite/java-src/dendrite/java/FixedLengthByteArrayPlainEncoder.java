package dendrite.java;

public class FixedLengthByteArrayPlainEncoder extends AbstractEncoder {

  private int length = -1;

  @Override
  public void encode(final Object o) {
    final byte[] bs = (byte[]) o;
    num_values += 1;
    if (length != bs.length) {
      if (length == -1) {
        length = bs.length;
        byte_array_writer.writeUInt(length);
      } else {
        throw new IllegalStateException("Different byte-array lengths in FixedLengthByteArrayEncoder");
      }
    }
    byte_array_writer.writeByteArray(bs, 0, length);
  }

  @Override
  public void reset() {
    super.reset();
    length = -1;
  }

}
