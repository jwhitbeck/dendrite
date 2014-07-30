package dendrite.java;

import java.util.List;

public interface Decoder extends Iterable {
  public Object decode();
  public int numEncodedValues();
}
