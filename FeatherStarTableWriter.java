package uk.ac.starlink.feather;

import jarrow.feather.FeatherColumnWriter;
import jarrow.feather.FeatherTableWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import uk.ac.starlink.table.ColumnInfo;
import uk.ac.starlink.table.StarTable;
import uk.ac.starlink.table.StreamStarTableWriter;

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
            ColumnInfo colInfo = table.getColumnInfo( ic );
            Class<?> clazz = colInfo.getContentClass();
            FeatherEncoder encoder = FeatherEncoders.getEncoder( clazz );
            if ( encoder != null ) {
                fcwList.add( new EncoderColumnWriter( table, ic, encoder ) );
            }
            else {
                logger_.warning( "Can't encode column " + colInfo + " to "
                               + getFormatName() + " format" );
            }
        }
        new FeatherTableWriter( description, tableMeta,
                                fcwList.toArray( new FeatherColumnWriter[0] ) )
           .write( out );
    }
}
