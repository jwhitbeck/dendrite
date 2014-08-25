package dendrite.java;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

public abstract class AbstractDecoder implements Decoder {

  protected final ByteArrayReader byteArrayReader;
  private final int numValues;

  public AbstractDecoder(final ByteArrayReader byteArrayReader) {
    this.byteArrayReader = byteArrayReader.slice();
    this.numValues = this.byteArrayReader.readUInt();
  }

  @Override
  public int numEncodedValues() {
    return numValues;
  }

  @Override
  public Iterator iterator() {
    return new Iterator() {
      int i = 0;
      @Override
      public boolean hasNext() {
        return i < numValues;
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
