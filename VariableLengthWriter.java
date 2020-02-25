package jarrow.feather;

import jarrow.fbs.feather.Type;
import java.io.IOException;
import java.io.OutputStream;
import java.util.logging.Logger;

public abstract class VariableLengthWriter extends AbstractColumnWriter {

    private final long nrow_;
    private static final Logger logger_ =
        Logger.getLogger( VariableLengthWriter.class.getName() );

    VariableLengthWriter( String name, byte type, long nrow,
                          boolean isNullable, String userMeta ) {
        super( name, type, nrow, isNullable, userMeta );
        nrow_ = nrow;
    }

    abstract int getByteSize( long irow );
    abstract void writeItem( long irow, OutputStream out )
            throws IOException;

    public long writeDataBytes( OutputStream out ) throws IOException {
        IndexStatus status = writeOffsets( out );
        long nbyteData = status.byteCount_;
        long nbyteOffsets = 4 * ( nrow_ + 1 );
        nbyteOffsets += BufUtils.align8( out, nbyteOffsets );
        long nentry = status.entryCount_;
        for ( long ir = 0; ir < nentry; ir++ ) {
            writeItem( ir, out );
        }
        nbyteData += BufUtils.align8( out, nbyteData );
        return nbyteOffsets + nbyteData;
    }

    public IndexStatus writeOffsets( OutputStream out ) throws IOException {
        long ioff = 0;
        for ( long ir = 0; ir < nrow_; ir++ ) {
            BufUtils.writeLittleEndianInt( out, (int) ioff );
            long ioff1 = ioff + getByteSize( ir );
            if ( ioff1 >= Integer.MAX_VALUE ) {
                logger_.warning( "Pointer overflow - empty values in column "
                               + getName() + " past row " + ir );
                IndexStatus status = new IndexStatus( ir, ioff );
                for ( ; ir < nrow_; ir++ ) {
                    BufUtils.writeLittleEndianInt( out, (int) ioff );
                }
                return status;
            }
            ioff = ioff1;
        }
        BufUtils.writeLittleEndianInt( out, (int) ioff );
        return new IndexStatus( nrow_, ioff );
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

    private static class IndexStatus {
        final long entryCount_;
        final long byteCount_;
        IndexStatus( long entryCount, long byteCount ) {
            entryCount_ = entryCount;
            byteCount_ = byteCount;
        }
    }
}
