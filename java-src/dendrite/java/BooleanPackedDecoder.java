package dendrite.java;

public class BooleanPackedDecoder implements BooleanDecoder {

  private final ByteArrayReader byte_array_reader;
  private final boolean[] octoplet = new boolean[8];
  private int position = 0;

  public BooleanPackedDecoder(final ByteArrayReader baw) {
    byte_array_reader = baw;
    byte_array_reader.readPackedBooleans(octoplet);
  }

  @Override
  public boolean next() {
    if (position == 8) {
      byte_array_reader.readPackedBooleans(octoplet);
      position = 0;
    }
    boolean b = octoplet[position];
    position += 1;
    return b;
  }

}
