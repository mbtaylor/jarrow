package uk.ac.starlink.feather;

import jarrow.fbs.feather.Type;
import jarrow.feather.BufUtils;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.logging.Logger;
import uk.ac.starlink.table.ByteStore;
import uk.ac.starlink.table.RowSequence;
import uk.ac.starlink.table.StarTable;
import uk.ac.starlink.table.StoragePolicy;

public abstract class VariableStarColumnWriter extends StarColumnWriter {

    private final PointerSize psize_;
    private static final Logger logger_ =
        Logger.getLogger( "uk.ac.starlink.feather" );

    protected VariableStarColumnWriter( StarTable table, int icol,
                                        byte featherType, boolean isNullable,
                                        PointerSize psize ) {
        super( table, icol, featherType, isNullable );
        psize_ = psize;
    }

    public abstract int getItemSize( Object item );
    public abstract int writeItemBytes( OutputStream out, Object item )
            throws IOException;

    public DataStat writeDataBytes( OutputStream out ) throws IOException {
        final int icol = getColumnIndex();

        /* Write offsets. */
        IndexStatus ixStat;
        RowSequence irseq = getTable().getRowSequence();
        try {
            ixStat = writeOffsets( out, irseq );
        }
        finally {
            irseq.close();
        }
        long ixb = psize_.nbyte_ * ( ixStat.rowCount_ + 1 );
        long indexBytes = ixb + BufUtils.align8( out, ixb );

        /* Write data. */
        long entryLimit = ixStat.entryCount_;
        RowSequence drseq = getTable().getRowSequence();
        try {
            for ( long ir = 0; drseq.next() && ir < entryLimit; ir++ ) {
                writeItemBytes( out, drseq.getCell( icol ) );
            }
        }
        finally {
            drseq.close();
        }
        long db = ixStat.byteCount_;
        long dataBytes = db + BufUtils.align8( out, db );

        long nbyte = indexBytes + dataBytes;
        long nrow = ixStat.rowCount_;
        return new DataStat( nbyte, nrow );
    }

    public ItemAccumulator createItemAccumulator( StoragePolicy storage ) {
        final ByteStore indexStore = storage.makeByteStore();
        final ByteStore dataStore = storage.makeByteStore();
        final OutputStream indexOut =
            new BufferedOutputStream( indexStore.getOutputStream() );
        final OutputStream dataOut =
            new BufferedOutputStream( dataStore.getOutputStream() );
        return new AbstractItemAccumulator( storage, isNullable() ) {
            long ioff;
            boolean hasOverflowed;
            long nrow;
            public void addDataItem( Object item ) throws IOException {
                nrow++;
                psize_.writeOffset( indexOut, ioff );
                long ioff1 = ioff + writeItemBytes( dataOut, item );
                if ( ! psize_.isOverflow( ioff1 ) ) {
                    ioff = ioff1;
                }
                else {
                    if ( ! hasOverflowed ) {
                        hasOverflowed = true;
                        logger_.warning( "Pointer overflow - "
                                       + "empty values in column "
                                       + getTable()
                                        .getColumnInfo( getColumnIndex() )
                                        .getName()
                                       + " past row " + nrow );
                    }
                }
            }
            public long writeDataBytes( OutputStream out ) throws IOException {
                psize_.writeOffset( indexOut, ioff );
                long ixb = psize_.nbyte_ * ( nrow + 1 );
                long indexBytes = ixb + BufUtils.align8( indexOut, ixb );
                indexOut.close();
                indexStore.copy( out );
                indexStore.close();
                dataOut.close();
                dataStore.copy( out );
                dataStore.close();
                return indexBytes + ioff;
            }
            public void closeData() throws IOException {
                indexOut.close();
                indexStore.close();
                dataOut.close();
                dataStore.close();
            }
        };
    }

    private IndexStatus writeOffsets( OutputStream out, RowSequence rseq )
            throws IOException {
        final int icol = getColumnIndex();
        long nrow = 0;
        long ioff = 0;
        while ( rseq.next() ) {
            psize_.writeOffset( out, ioff );
            long ioff1 = ioff + getItemSize( rseq.getCell( icol ) );
            if ( psize_.isOverflow( ioff1 ) ) {
                logger_.warning( "Pointer overflow - empty values in column "
                               + getTable().getColumnInfo( icol ).getName()
                               + " past row " + nrow );
                long entryCount = nrow;
                do {
                    psize_.writeOffset( out, ioff );
                    nrow++;
                } while ( rseq.next() );
                return new IndexStatus( nrow, entryCount, ioff );
            }
            ioff = ioff1;
            nrow++;
        }
        psize_.writeOffset( out, ioff );
        return new IndexStatus( nrow, nrow, ioff );
    }

    public static VariableStarColumnWriter
            createStringWriter( StarTable table, int icol,
                                boolean isNullable, PointerSize psize ) {
        return new VariableStarColumnWriter( table, icol, psize.utf8Type_,
                                             isNullable, psize ) {
            public int getItemSize( Object item ) {
                return item == null
                     ? 0
                     : BufUtils.utf8Length( item.toString() );
            }
            public int writeItemBytes( OutputStream out, Object item )
                    throws IOException {
                if ( item != null ) {
                    byte[] bytes = item.toString().getBytes( BufUtils.UTF8 );
                    out.write( bytes );
                    return bytes.length;
                }
                else {
                    return 0;
                }
            }
        };
    }

    public static VariableStarColumnWriter
            createByteArrayWriter( StarTable table, int icol,
                                   boolean isNullable, PointerSize psize ) {
        return new VariableStarColumnWriter( table, icol, psize.binaryType_,
                                             isNullable, psize ) {
            public int getItemSize( Object item ) {
                return item instanceof byte[]
                     ? ((byte[]) item).length
                     : 0;
            }
            public int writeItemBytes( OutputStream out, Object item )
                    throws IOException {
                if ( item instanceof byte[] ) {
                    byte[] bytes = (byte[]) item;
                    out.write( bytes );
                    return bytes.length;
                }
                else {
                    return 0;
                }
            }
        };
    }

    public enum PointerSize {

        I32( 4, Type.UTF8, Type.BINARY ) {
            void writeOffset( OutputStream out, long ioff ) throws IOException {
                BufUtils.writeLittleEndianInt( out, (int) ioff );
            }
            boolean isOverflow( long ioff ) {
                return ioff >= Integer.MAX_VALUE;
            }
        },
        I64( 8, Type.LARGE_UTF8, Type.LARGE_BINARY ) {
            void writeOffset( OutputStream out, long ioff ) throws IOException {
                BufUtils.writeLittleEndianLong( out, ioff );
            }
            boolean isOverflow( long ioff ) {
                return false;
            }
        };

        final int nbyte_;
        final byte utf8Type_;
        final byte binaryType_;

        private PointerSize( int nbyte, byte utf8Type, byte binaryType ) {
            nbyte_ = nbyte;
            utf8Type_ = utf8Type;
            binaryType_ = binaryType;
        }
        abstract void writeOffset( OutputStream out, long ioff )
                throws IOException;
        abstract boolean isOverflow( long ioff );
    }

    private static class IndexStatus {
        final long rowCount_;
        final long entryCount_;
        final long byteCount_;
        IndexStatus( long rowCount, long entryCount, long byteCount ) {
            rowCount_ = rowCount;
            entryCount_ = entryCount;
            byteCount_ = byteCount;
        }
    }
}
