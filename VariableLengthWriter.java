package jarrow.feather;

import jarrow.fbs.Type;
import java.io.IOException;
import java.io.OutputStream;

public abstract class VariableLengthWriter extends AbstractColumnWriter {
    private final long nrow_;
    VariableLengthWriter( String name, byte type, long nrow,
                          boolean isNullable, String userMeta ) {
        super( name, type, nrow, isNullable, userMeta );
        nrow_ = nrow;
    }

    abstract int getByteSize( long irow );
    abstract void writeItem( long irow, OutputStream out )
            throws IOException;

    public long writeDataBytes( OutputStream out ) throws IOException {
        int ioff = 0;
        for ( int ir = 0; ir < nrow_; ir++ ) {
            BufUtils.writeLittleEndianInt( out, ioff );
            ioff += getByteSize( ir );
        }
        BufUtils.writeLittleEndianInt( out, ioff );
        long nbyteIndex = 4 * ( nrow_ + 1 );
        nbyteIndex += BufUtils.align8( out, nbyteIndex );
        long nbyteData = ioff;
        for ( int ir = 0; ir < nrow_; ir++ ) {
            writeItem( ir, out );
        }
        nbyteData += BufUtils.align8( out, nbyteData );
        return nbyteIndex + nbyteData;
    }

    public static FeatherColumnWriter
            createStringWriter( String name, final String[] data,
                                String userMeta, boolean isNullable ) {
        return new VariableLengthWriter( name, Type.UTF8, data.length,
                                         isNullable, userMeta ) {
            public boolean isNull( long irow ) {
                return data[ BufUtils.longToInt( irow ) ] == null;
            }
            public int getByteSize( long irow ) {
                String str = data[ BufUtils.longToInt( irow ) ];
                return str == null ? 0 : BufUtils.utf8Length( str );
            }
            public void writeItem( long irow, OutputStream out )
                    throws IOException {
                String str = data[ BufUtils.longToInt( irow ) ];
                if ( str != null ) {
                    out.write( str.getBytes( BufUtils.UTF8 ) );
                }
            }
        };
    }
}
