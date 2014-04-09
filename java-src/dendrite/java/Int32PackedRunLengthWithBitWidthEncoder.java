package dendrite.java;

public class Int32PackedRunLengthWithBitWidthEncoder extends Int32PackedRunLengthEncoder {

  public Int32PackedRunLengthWithBitWidthEncoder(final int width) {
    super(width);
    byte_array_writer.writeByte((byte)width);
  }

  @Override
  public void reset() {
    super.reset();
    byte_array_writer.writeByte((byte)width);
  }

}
