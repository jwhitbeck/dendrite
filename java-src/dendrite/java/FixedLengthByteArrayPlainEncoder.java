package dendrite.java;

public class FixedLengthByteArrayPlainEncoder extends AbstractEncoder implements FixedLengthByteArrayEncoder {

  final private int length;

  public FixedLengthByteArrayPlainEncoder(final int length) {
    this.length = length;
  }

  @Override
  public void encode(final byte[] bs) {
    byte_array_writer.writeByteArray(bs, 0, length);
  }

}
