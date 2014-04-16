package dendrite.java;

public class Int32FixedBitWidthPackedRunLengthDecoder implements Int32Decoder {

  private final ByteArrayReader byte_array_reader;
  private final int[] octuplet = new int[8];
  private int octuplet_position = 8;
  private int num_octoplets_to_read = 0;
  private int rle_value = 0;
  private int num_rle_values_to_read = 0;
  private final int width;

  public Int32FixedBitWidthPackedRunLengthDecoder(final ByteArrayReader baw, final int width) {
    byte_array_reader = baw;
    this.width = width;
  }

  @Override
  public int decode() {
    if (num_rle_values_to_read > 0) {
      return decodeFromRLEValue();
    } else if (octuplet_position < 8) {
      return decodeFromOctuplet();
    } else {
      bufferNextRun();
      return decode();
    }
  }

  private int decodeFromOctuplet() {
    int v = octuplet[octuplet_position];
    octuplet_position += 1;
    if (octuplet_position == 8 && num_octoplets_to_read > 0) {
      bufferNextOctuplet();
    }
    return v;
  }

  private int decodeFromRLEValue() {
    num_rle_values_to_read -= 1;
    return rle_value;
  }

  private void bufferNextOctuplet() {
    byte_array_reader.readPackedInts32(octuplet, width, 8);
    num_octoplets_to_read -= 1;
    octuplet_position = 0;
  }

  private void bufferNextRLERun(final int num_occurences_rle_value) {
    num_rle_values_to_read = num_occurences_rle_value;
    rle_value = byte_array_reader.readUInt32();
  }

  private void bufferNextPackedIntRun(final int num_octuplets) {
    num_octoplets_to_read = num_octuplets;
    bufferNextOctuplet();
  }

  private void bufferNextRun() {
    int n = byte_array_reader.readUInt32();
    if ((n & 1) == 1) {
      bufferNextPackedIntRun(n >>> 1);
    } else {
      bufferNextRLERun(n >>> 1);
    }
  }

}
