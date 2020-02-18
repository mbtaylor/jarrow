package uk.ac.starlink.feather;

import java.io.IOException;
import java.io.OutputStream;

public interface FeatherEncoder {

    public byte getFeatherType();

    public boolean isNullable();
    public boolean isVariableLength();

    /** Only called if isNullable is true. */
    boolean isNull( Object value );

    /** Only called if isVariableLength is true.
     *  Must be consistent with writeBytes*/
    int getByteSize( Object value );

    int writeBytes( OutputStream out, Object value ) throws IOException;
}
