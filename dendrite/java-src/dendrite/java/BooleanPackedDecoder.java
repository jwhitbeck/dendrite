package dendrite.java;

public class BooleanPackedDecoder implements BooleanDecoder {

  private final ByteArrayReader byte_array_reader;
  private final boolean[] octuplet = new boolean[8];
  private int position = 0;

  public BooleanPackedDecoder(final ByteArrayReader baw) {
    byte_array_reader = baw;
    byte_array_reader.readPackedBooleans(octuplet);
  }

  @Override
  public boolean decode() {
    if (position == 8) {
      byte_array_reader.readPackedBooleans(octuplet);
      position = 0;
    }
    boolean b = octuplet[position];
    position += 1;
    return b;
  }

}
