// automatically generated by the FlatBuffers compiler, do not modify

package jarrow.fbs.arrow;

public final class MetadataVersion {
  private MetadataVersion() { }
  /**
   * 0.1.0
   */
  public static final short V1 = 0;
  /**
   * 0.2.0
   */
  public static final short V2 = 1;
  /**
   * 0.3.0 -> 0.7.1
   */
  public static final short V3 = 2;
  /**
   * >= 0.8.0
   */
  public static final short V4 = 3;

  public static final String[] names = { "V1", "V2", "V3", "V4", };

  public static String name(int e) { return names[e]; }
}
