package dendrite.java;

public class ByteArrayPlainEncoder extends AbstractEncoder {

  @Override
  public void encode(final Object o) {
    final byte[] bs = (byte[]) o;
    num_values += 1;
    byte_array_writer.writeFixedInt(bs.length);
    byte_array_writer.writeByteArray(bs, 0, bs.length);
  }

}
