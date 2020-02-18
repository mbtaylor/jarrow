package jarrow.feather;

import jarrow.fbs.Type;
import java.io.IOException;
import java.io.OutputStream;

public abstract class AbstractColumnWriter implements FeatherColumnWriter {

    private final String name_;
    private final byte featherType_;
    private final long nrow_;
    private final boolean isNullable_;
    private final String userMeta_;

    protected AbstractColumnWriter( String name, byte featherType, long nrow,
                                    boolean isNullable, String userMeta ) {
        name_ = name;
        featherType_ = featherType;
        nrow_ = nrow;
        isNullable_ = isNullable;
        userMeta_ = userMeta;
    }

    public String getName() {
        return name_;
    }

    public byte getFeatherType() {
        return featherType_;
    }

    public long getRowCount() {
        return nrow_;
    }

    public boolean isNullable() {
        return isNullable_;
    }

    public String getUserMetadata() {
        return userMeta_;
    }

    public ColStat writeColumnBytes( OutputStream out ) throws IOException {
        long nNull = 0;
        final long maskBytes;
        if ( isNullable_ ) {
            int mask = 0;
            int ibit = 0;
            for ( long ir = 0; ir < nrow_; ir++ ) {
                if ( isNull( ir ) ) {
                    nNull++;
                    mask |= 1 << ibit++;
                }
                if ( ibit == 8 ) {
                    out.write( mask );
                    ibit = 0;
                    mask = 0;
                }
            }
            if ( ibit > 0 ) {
                out.write( mask );
            }
            long mb = ( nrow_ + 7 ) / 8;
            maskBytes = mb + BufUtils.align8( out, mb );
        }
        else {
            maskBytes = 0;
        }
        long dataBytes = writeDataBytes( out );
        dataBytes += BufUtils.align8( out, dataBytes );
        boolean hasNull = nNull > 0;
        final long byteCount = hasNull ? maskBytes + dataBytes : dataBytes;
        final long dataOffset = hasNull ? 0 : maskBytes;
        final long nullCount = nNull;
        return new ColStat() {
            public long getRowCount() {
                return nrow_;
            }
            public long getByteCount() {
                return byteCount;
            }
            public long getDataOffset() {
                return dataOffset;
            }
            public long getNullCount() {
                return nullCount;
            }
        };
    }

    // Only called if isNullable returns true
    public abstract boolean isNull( long irow );

    // Excluding any mask.  Doesn't need to be aligned.
    // @return   number of bytes written
    public abstract long writeDataBytes( OutputStream out ) throws IOException;
}
