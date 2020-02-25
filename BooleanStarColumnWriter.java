package uk.ac.starlink.feather;

import jarrow.fbs.feather.Type;
import java.io.IOException;
import java.io.OutputStream;
import uk.ac.starlink.table.RowSequence;
import uk.ac.starlink.table.StarTable;

public class BooleanStarColumnWriter extends StarColumnWriter {

    public BooleanStarColumnWriter( StarTable table, int icol ) {
        super( table, icol, Type.BOOL, true );
    }

    public DataStat writeDataBytes( OutputStream out ) throws IOException {
        final int icol = getColumnIndex();
        RowSequence rseq = getTable().getRowSequence();
        int mask = 0;
        int ibit = 0;
        long nrow = 0;
        try {
            while ( rseq.next() ) {
                nrow++;
                if ( Boolean.TRUE.equals( rseq.getCell( icol ) ) ) {
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
        long nbyte = ( nrow + 7 ) / 8;
        return new DataStat( nbyte, nrow );
    }
}
