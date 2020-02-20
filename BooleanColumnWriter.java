package jarrow.feather;

import jarrow.fbs.Type;
import java.io.IOException;
import java.io.OutputStream;

public abstract class BooleanColumnWriter extends AbstractColumnWriter {

    protected BooleanColumnWriter( String name, long nrow, boolean isNullable,
                                   String userMeta ) {
        super( name, Type.BOOL, nrow, isNullable, userMeta );
    }

    public abstract boolean getValue( long ix );

    /**
     * Returns false, but may be overridden if isNullable is true.
     */
    public boolean isNull( long irow ) {
        return false;
    }

    public long writeDataBytes( OutputStream out ) throws IOException {
        long nrow = getRowCount();
        int outByte = 0;
        int ibit = 0;
        for ( long ir = 0; ir < nrow; ir++ ) {
            if ( getValue( ir ) ) {
                outByte |= 1 << ibit;
            }
            if ( ++ibit == 8 ) {
                out.write( outByte );
                ibit = 0;
                outByte = 0;
            }
        }
        if ( ibit > 0 ) {
            out.write( outByte );
        }
        return ( nrow + 7 ) / 8;
    }

    public static BooleanColumnWriter
            createBooleanWriter( String name, final boolean[] data,
                                 String userMeta ) {
        return new BooleanColumnWriter( name, data.length, false, userMeta ) {
            public boolean getValue( long ix ) {
                return data[ BufUtils.longToInt( ix ) ];
            }
        };
    }
}
