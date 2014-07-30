package dendrite.java;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

public abstract class AbstractDecoder implements Decoder {

  protected final ByteArrayReader byte_array_reader;
  private final int num_values;

  public AbstractDecoder(final ByteArrayReader baw) {
    byte_array_reader = baw.slice();
    num_values = byte_array_reader.readUInt();
  }

  @Override
  public int numEncodedValues() {
    return num_values;
  }

  @Override
  public Iterator iterator() {
    return new Iterator() {
      int i = 0;
      @Override
      public boolean hasNext() {
        return i < num_values;
      }
      @Override
      public Object next() {
        Object v = decode();
        i += 1;
        return v;
      }
      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }
}
