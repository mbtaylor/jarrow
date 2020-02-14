package jarrow.feather;

import jarrow.fbs.Type;
import java.io.IOException;
import java.io.OutputStream;

public interface FeatherColumnWriter {

    String getName();
    byte getFeatherType();
    String getUserMetadata();

    // Must align to 64-bit boundary
    ColStat writeColumnBytes( OutputStream out ) throws IOException;
}
