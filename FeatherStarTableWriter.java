package uk.ac.starlink.feather;

import jarrow.feather.ColStat;
import jarrow.feather.FeatherColumnWriter;
import jarrow.feather.FeatherTableWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import uk.ac.starlink.table.RowSequence;
import uk.ac.starlink.table.StarTable;
import uk.ac.starlink.table.StoragePolicy;
import uk.ac.starlink.table.StreamStarTableWriter;
import uk.ac.starlink.table.Tables;
import uk.ac.starlink.util.IntList;

public class FeatherStarTableWriter extends StreamStarTableWriter {

    private final boolean isColumnOrder_;
    private final StoragePolicy storage_;
    private static final Logger logger_ =
        Logger.getLogger( "uk.ac.starlink.feather" );

    public FeatherStarTableWriter() {
        this( false, StoragePolicy.getDefaultPolicy() );
    }

    public FeatherStarTableWriter( boolean isColumnOrder,
                                   StoragePolicy storage ) {
        isColumnOrder_ = isColumnOrder;
        storage_ = storage;
    }

    public String getFormatName() {
        return "feather";
    }

    public String getMimeType() {
        return "application/octet-stream";
    }

    public boolean looksLikeFile( String loc ) {
        int idot = loc.lastIndexOf( '.' );
        String extension = idot >= 0 ? loc.substring( idot ) : "";
        return extension.equalsIgnoreCase( ".fea" )
            || extension.equalsIgnoreCase( ".feather" );
    }

    public void writeStarTable( StarTable table, OutputStream out )
            throws IOException {
        String description = table.getName();
        String tableMeta = null;
        int ncol = table.getColumnCount();
        List<StarColumnWriter> cwList = new ArrayList<>();
        IntList icList = new IntList();
        for ( int ic = 0; ic < ncol; ic++ ) {
            StarColumnWriter writer =
                StarColumnWriters.createColumnWriter( table, ic );
            if ( writer != null ) {
                icList.add( ic );
                cwList.add( writer );
            }
            else {
                logger_.warning( "Can't encode column "
                               + table.getColumnInfo( ic ) + " to "
                               + getFormatName() + " format" );
            }
        }
        final FeatherColumnWriter[] colWriters;
        if ( isColumnOrder_ ) {
            colWriters = cwList.toArray( new FeatherColumnWriter[ 0 ] );
        }
        else {
            int[] ics = icList.toIntArray();
            int nic = ics.length;
            ItemAccumulator[] accs = new ItemAccumulator[ nic ];
            for ( int jc = 0; jc < nic; jc++ ) {
                int ic = ics[ jc ];
                accs[ jc ] = cwList.get( jc ).createItemAccumulator( storage_ );
            }
            RowSequence rseq = table.getRowSequence();
            while ( rseq.next() ) {
                Object[] row = rseq.getRow();
                for ( int jc = 0; jc < nic; jc++ ) {
                    int ic = ics[ jc ];
                    accs[ jc ].addItem( row[ ic ] );
                }
            }
            colWriters = new FeatherColumnWriter[ nic ];
            for ( int jc = 0; jc < nic; jc++ ) {
                final FeatherColumnWriter cw = cwList.get( jc );
                final ItemAccumulator acc = accs[ jc ];
                colWriters[ jc ] = new FeatherColumnWriter() {
                    public byte getFeatherType() {
                        return cw.getFeatherType();
                    }
                    public String getName() {
                        return cw.getName();
                    }
                    public String getUserMetadata() {
                        return cw.getUserMetadata();
                    }
                    public ColStat writeColumnBytes( OutputStream out )
                            throws IOException {
                        return acc.writeColumnBytes( out );
                    }
                };
            }
        }
        new FeatherTableWriter( description, tableMeta, colWriters )
           .write( out );
    }
}
