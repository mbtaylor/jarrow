package uk.ac.starlink.feather;

import jarrow.feather.BufUtils;
import jarrow.feather.ColStat;
import jarrow.feather.FeatherColumnWriter;
import jarrow.feather.FeatherType;
import java.io.IOException;
import java.io.OutputStream;
import org.json.JSONObject;
import uk.ac.starlink.table.ColumnInfo;
import uk.ac.starlink.table.DefaultValueInfo;
import uk.ac.starlink.table.RowSequence;
import uk.ac.starlink.table.StarTable;
import uk.ac.starlink.table.StoragePolicy;

public abstract class StarColumnWriter implements FeatherColumnWriter {

    private final StarTable table_;
    private final int icol_;
    private final FeatherType featherType_;
    private final boolean isNullable_;

    /**
     * The <code>isNullable</code> parameter only needs to be given true
     * if the writeData method cannot represent null values in its
     * byte representation.
     *
     * @param  isNullable  contains any null values which should be marked
     *                     as such in the null values mask
     */
    protected StarColumnWriter( StarTable table, int icol,
                                FeatherType featherType, boolean isNullable ) {
        table_ = table;
        icol_ = icol;
        featherType_ = featherType;
        isNullable_ = isNullable;
    }

    // Excluding any mask.  Doesn't need to be aligned.
    public abstract DataStat writeDataBytes( OutputStream out )
            throws IOException;

    public StarTable getTable() {
        return table_;
    }

    public int getColumnIndex() {
        return icol_;
    }

    public FeatherType getFeatherType() {
        return featherType_;
    }

    public boolean isNullable() {
        return isNullable_;
    }

    public String getName() {
        return table_.getColumnInfo( icol_ ).getName();
    }

    public String getUserMetadata() {
        ColumnInfo info = table_.getColumnInfo( icol_ );
        JSONObject json = new JSONObject();
        addEntry( json, FeatherStarTable.UNIT_KEY, info.getUnitString() );
        addEntry( json, FeatherStarTable.UCD_KEY, info.getUCD() );
        addEntry( json, FeatherStarTable.UTYPE_KEY, info.getUtype() );
        addEntry( json, FeatherStarTable.DESCRIPTION_KEY,
                  info.getDescription() );
        addEntry( json, FeatherStarTable.SHAPE_KEY,
                  DefaultValueInfo.formatShape( info.getShape() ) );
        return json.length() > 0
             ? json.toString( 0 ).replaceAll( "\n", " " )
             : null;
    }

    public ColStat writeColumnBytes( OutputStream out ) throws IOException {

        /* Write mask, if applicable. */
        long nNull = 0;
        final long maskBytes;
        if ( isNullable_ && table_.getColumnInfo( icol_ ).isNullable() ) {
            int mask = 0;
            int ibit = 0;
            long nrow = 0;
            RowSequence rseq = table_.getRowSequence();
            try {
                while ( rseq.next() ) {
                    nrow++;
                    if ( rseq.getCell( icol_ ) == null ) {
                        nNull++;
                    }
                    else {
                        mask |= 1 << ibit;
                    }
                    if ( ++ibit == 8 ) {
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

        /* Write data. */
        DataStat dataStat = writeDataBytes( out );

        /* Package and return statistics. */
        long db = dataStat.getByteCount();
        final long rowCount = dataStat.getRowCount();
        long dataBytes = db + BufUtils.align8( out, db );
        boolean hasNull = nNull > 0;
        final long byteCount = maskBytes + dataBytes;
        final long dataOffset = maskBytes;
        final long nullCount = nNull;
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

    public abstract ItemAccumulator
        createItemAccumulator( StoragePolicy storage );

    private static void addEntry( JSONObject json, String key, String value ) {
        if ( value != null && value.trim().length() > 0 ) {
            json.put( key, value );
        }
    }

    public class DataStat {
        private final long byteCount_;
        private final long rowCount_;
        public DataStat( long byteCount, long rowCount ) {
            byteCount_ = byteCount;
            rowCount_ = rowCount;
        }
        public long getByteCount() {
            return byteCount_;
        }
        public long getRowCount() {
            return rowCount_;
        }
    }
}
