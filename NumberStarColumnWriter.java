package uk.ac.starlink.feather;

import java.io.IOException;
import java.io.OutputStream;
import uk.ac.starlink.table.RowSequence;
import uk.ac.starlink.table.StarTable;

public abstract class NumberStarColumnWriter extends StarColumnWriter {

    private final byte[] blank_;
    private final int itemSize_;

    public NumberStarColumnWriter( StarTable table, int icol,
                                   byte featherType, boolean isNullable,
                                   byte[] blank ) {
        super( table, icol, featherType, isNullable );
        blank_ = blank.clone();
        itemSize_ = blank.length;
    }

    // value not null
    public abstract void writeNumber( OutputStream out, Number value )
            throws IOException;

    public DataStat writeDataBytes( OutputStream out ) throws IOException {
        final int icol = getColumnIndex();
        RowSequence rseq = getTable().getRowSequence();
        long nrow = 0;
        try {
            while ( rseq.next() ) {
                nrow++;
                Object item = rseq.getCell( icol );
                if ( item != null ) {
                    writeNumber( out, (Number) item );
                }
                else {
                    out.write( blank_ );
                }
            }
        }
        finally {
            rseq.close();
        }
        long nbyte = nrow * itemSize_;
        return new DataStat( nbyte, nrow );
    }
}
