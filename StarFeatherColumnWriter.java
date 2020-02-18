package uk.ac.starlink.feather;

import jarrow.feather.BufUtils;
import jarrow.feather.ColStat;
import jarrow.feather.FeatherColumnWriter;
import java.io.IOException;
import java.io.OutputStream;
import org.json.JSONObject;
import uk.ac.starlink.table.ColumnInfo;
import uk.ac.starlink.table.RowSequence;
import uk.ac.starlink.table.StarTable;

public class StarFeatherColumnWriter implements FeatherColumnWriter {

    private final StarTable table_;
    private final int icol_;
    private final FeatherEncoder encoder_;

    public StarFeatherColumnWriter( StarTable table, int icol,
                                    FeatherEncoder encoder ) {
        table_ = table;
        icol_ = icol;
        encoder_ = encoder;
    }

    public String getName() {
        return table_.getColumnInfo( icol_ ).getName();
    }                        

    public byte getFeatherType() {
        return encoder_.getFeatherType();
    }

    public String getUserMetadata() {
        ColumnInfo info = table_.getColumnInfo( icol_ );
        JSONObject json = new JSONObject();
        addEntry( json, "unit", info.getUnitString() );
        addEntry( json, "ucd", info.getUCD() );
        addEntry( json, "utype", info.getUtype() );
        addEntry( json, "description", info.getDescription() );
        return json.length() > 0
             ? json.toString( 0 ).replaceAll( "\n", "" )
             : null;
    }

    public ColStat writeColumnBytes( OutputStream out ) throws IOException {


        /* Write mask, if applicable. */
        long nnull = 0;
        final long maskBytes;
        boolean isNullable = encoder_.isNullable()
                          && table_.getColumnInfo( icol_ ).isNullable();
        if ( isNullable ) {
            int mask = 0;
            int ibit = 0;
            long nrow = 0;
            RowSequence rseq = table_.getRowSequence();
            try {
                while ( rseq.next() ) {
                    nrow++;
                    if ( encoder_.isNull( rseq.getCell( icol_ ) ) ) {
                        nnull++;
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
            }
            finally {
                rseq.close();
            }
            long mb = ( nrow + 7 ) / 8;
            maskBytes = mb + BufUtils.align8( out, mb );
        }
        else {
            maskBytes = 0;
        }

        /* Write offset array, if applicable. */
        final long indexBytes;
        if ( encoder_.isVariableLength() ) {
            long nrow = 0;
            RowSequence rseq = table_.getRowSequence();
            try {
                int ioff = 0;
                while ( rseq.next() ) {
                    BufUtils.writeLittleEndianInt( out, ioff );
                    ioff += encoder_.getByteSize( rseq.getCell( icol_ ) );
                    nrow++;
                }
                BufUtils.writeLittleEndianInt( out, ioff );
            }
            finally {
                rseq.close();
            }
            long ixb = 4 * ( nrow + 1 );
            indexBytes = ixb + BufUtils.align8( out, ixb );
        }
        else {
            indexBytes = 0;
        }

        /* Write data. */
        long nrow = 0;
        long dataBytes = 0;
        RowSequence rseq = table_.getRowSequence();
        try {
            while ( rseq.next() ) {
                nrow++;
                dataBytes += encoder_.writeBytes( out, rseq.getCell( icol_ ) );
            }
        }
        finally {
            rseq.close();
        }
        dataBytes += BufUtils.align8( out, dataBytes );
        boolean hasNull = nnull > 0;

        /* Prepare and return summary. */
        final long rowCount = nrow;
        final long dataOffset = hasNull ? 0 : maskBytes;
        final long byteCount = maskBytes + indexBytes + dataBytes - dataOffset;
        final long nullCount = nnull;
        return new ColStat() {
            public long getRowCount() {
                return rowCount;
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

    private static void addEntry( JSONObject json, String key, String value ) {
        if ( value != null && value.trim().length() > 0 ) {
            json.put( key, value );
        }
    }   
}
