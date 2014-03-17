package dendrite.java;

public class BooleanPackedEncoder extends AbstractEncoder implements BooleanEncoder {

  private boolean[] octoplet = new boolean[8];
  private int position = 0;

  @Override
  public void append(boolean b) {
    octoplet[position] = b;
    position += 1;
    if (position == 8) {
      byte_array_writer.writePackedBooleans(octoplet);
      position = 0;
    }
  }

  @Override
  public void reset() {
    position = 0;
    super.reset();
  }

  @Override
  public void flush(final ByteArrayWriter baw) {
    while (position > 0) {
      append(false);
    }
    super.flush(baw);
  }

}
