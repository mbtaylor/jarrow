package uk.ac.starlink.feather;

import jarrow.feather.FeatherColumnWriter;
import jarrow.feather.FeatherTableWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import uk.ac.starlink.table.StarTable;
import uk.ac.starlink.table.StreamStarTableWriter;
import uk.ac.starlink.table.Tables;

public class FeatherStarTableWriter extends StreamStarTableWriter {

    private static final Logger logger_ =
        Logger.getLogger( "uk.ac.starlink.feather" );

    public FeatherStarTableWriter() {
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
        List<FeatherColumnWriter> fcwList = new ArrayList<>();
        for ( int ic = 0; ic < ncol; ic++ ) {
            FeatherColumnWriter writer =
                StarColumnWriters.createColumnWriter( table, ic );
            if ( writer != null ) {
                fcwList.add( writer );
            }
            else {
                logger_.warning( "Can't encode column "
                               + table.getColumnInfo( ic ) + " to "
                               + getFormatName() + " format" );
            }
        }
        new FeatherTableWriter( description, tableMeta,
                                fcwList.toArray( new FeatherColumnWriter[0] ) )
           .write( out );
    }
}
