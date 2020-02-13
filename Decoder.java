package jarrow.feather;

import java.nio.ByteBuffer;

public abstract class Decoder {
    public abstract Reader createReader( ByteBuffer buf );

    public static Decoder createDecoder( byte type, boolean hasNulls ) {
  return null;
    }
}
