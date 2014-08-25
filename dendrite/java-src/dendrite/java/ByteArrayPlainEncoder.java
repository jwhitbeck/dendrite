package dendrite.java;

public class ByteArrayPlainEncoder extends AbstractEncoder {

  @Override
  public void encode(final Object o) {
    final byte[] bs = (byte[]) o;
    numValues += 1;
    byteArrayWriter.writeFixedInt(bs.length);
    byteArrayWriter.writeByteArray(bs, 0, bs.length);
  }

}
