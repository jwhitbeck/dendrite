package dendrite.java;

public interface Encoder extends Resetable {

  public void flush(final ByteArrayWriter baw);

}
