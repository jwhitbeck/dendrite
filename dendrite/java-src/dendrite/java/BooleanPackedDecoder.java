package dendrite.java;

public class BooleanPackedDecoder extends AbstractDecoder {

  private final boolean[] octuplet = new boolean[8];
  private int position = 0;

  public BooleanPackedDecoder(final ByteArrayReader baw){
    super(baw);
  }

  @Override
  public Object decode() {
    if ((position % 8) == 0) {
      byteArrayReader.readPackedBooleans(octuplet);
      position = 0;
    }
    boolean b = octuplet[position];
    position += 1;
    return b;
  }

}
