package uk.ac.starlink.feather;

import jarrow.feather.BufUtils;
import jarrow.feather.ColStat;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import uk.ac.starlink.table.ByteStore;
import uk.ac.starlink.table.StoragePolicy;

public abstract class AbstractItemAccumulator implements ItemAccumulator {

    private final StoragePolicy storage_;
    private final boolean isNullable_;
    private final ByteStore maskStore_;
    private final OutputStream maskOut_;
    private long nRow_;
    private long nNull_;
    private int ibit_;
    private int mask_;

    protected AbstractItemAccumulator( StoragePolicy storage,
                                       boolean isNullable ) {
        storage_ = storage;
        isNullable_ = isNullable;
        maskStore_ = isNullable ? storage.makeByteStore() : null;
        maskOut_ = isNullable
                 ? new BufferedOutputStream( maskStore_.getOutputStream() )
                 : null;
    }

    public abstract void addDataItem( Object item ) throws IOException;
    // doesn't need to be aligned
    public abstract long writeDataBytes( OutputStream out ) throws IOException;
    public abstract void closeData() throws IOException;

    public void close() throws IOException {
        if ( isNullable_ ) {
            maskOut_.close();
            maskStore_.close();
        }
        closeData();
    }

    public void addItem( Object item ) throws IOException {
        nRow_++;
        if ( isNullable_ ) {
            if ( item == null ) {
                nNull_++;
            }
            else {
                mask_ |= 1 << ibit_;
            }
            if ( ++ibit_ == 8 ) {
                maskOut_.write( mask_ );
                ibit_ = 0;
                mask_ = 0;
            }
        }
        addDataItem( item );
    }

    public ColStat writeColumnBytes( OutputStream out ) throws IOException {
        final long nNullByte;
        if ( isNullable_ ) {
            if ( ibit_ > 0 ) {
                maskOut_.write( mask_ );
            }
            long nbMask = ( nRow_ + 7 ) / 8;
            nbMask += BufUtils.align8( maskOut_, nbMask );
            maskOut_.close();
            if ( nNull_ > 0 ) {
                maskStore_.copy( out );
                nNullByte = nbMask;
            }
            else {
                nNullByte = 0;
            }
            maskStore_.close();
        }
        else {
            nNullByte = 0;
        }
        long nbData = writeDataBytes( out );
        long nDataByte = nbData + BufUtils.align8( out, nbData );

        final long rowCount = nRow_;
        final long nullCount = nNull_;
        final long byteCount;
        final long dataOffset;
        if ( nNull_ > 0 ) {
            byteCount = nNullByte + nDataByte;
            dataOffset = nNullByte;
        }
        else {
            byteCount = nDataByte;
            dataOffset = 0;
        }
        return new ColStat() {
            public long getByteCount() {
                return byteCount;
            }
            public long getNullCount() {
                return nullCount;
            }
            public long getRowCount() {
                return rowCount;
            }
            public long getDataOffset() {
                return dataOffset;
            }
        };
    }
}
