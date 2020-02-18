package uk.ac.starlink.feather;

import java.awt.datatransfer.DataFlavor;
import jarrow.feather.FeatherTable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import uk.ac.starlink.table.StarTable;
import uk.ac.starlink.table.StoragePolicy;
import uk.ac.starlink.table.TableBuilder;
import uk.ac.starlink.table.TableFormatException;
import uk.ac.starlink.table.TableSink;
import uk.ac.starlink.util.Compression;
import uk.ac.starlink.util.DataSource;
import uk.ac.starlink.util.FileDataSource;

public class FeatherTableBuilder implements TableBuilder {

    public FeatherTableBuilder() {
    }

    public String getFormatName() {
        return "feather";
    }

    public StarTable makeStarTable( DataSource datsrc, boolean wantRandom,
                                    StoragePolicy storagePolicy )
            throws IOException {
        if ( ! FeatherTable.isMagic( datsrc.getIntro() ) ) {
            throw new TableFormatException( "No FEA1 magic number" );
        }
        if ( datsrc instanceof FileDataSource &&
             datsrc.getCompression() == Compression.NONE ) {
            File ffile = ((FileDataSource) datsrc).getFile();
            return new FeatherStarTable( FeatherTable.fromFile( ffile ) );
        }
        else {
            throw new TableFormatException( "Only uncompressed files supported"
                                          + " for Feather" );
        }
    }

    public boolean canImport( DataFlavor flavor ) {
        return false;
    }

    public void streamStarTable( InputStream in, TableSink sink, String pos )
            throws IOException {
        throw new TableFormatException( "Can't stream from Feather format" );
    }
}
