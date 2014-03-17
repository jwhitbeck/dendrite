package dendrite.java;

public class ByteArrayPlainEncoder extends AbstractEncoder implements ByteArrayEncoder {

  @Override
  public void append(byte[] bs) {
    byte_array_writer.writeFixedInt32(bs.length);
    byte_array_writer.writeByteArray(bs, 0, bs.length);
  }

}
