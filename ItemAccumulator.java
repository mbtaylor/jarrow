package uk.ac.starlink.feather;

import jarrow.feather.ColStat;
import java.io.IOException;
import java.io.OutputStream;

public interface ItemAccumulator {
    void addItem( Object item ) throws IOException;
    ColStat writeColumnBytes( OutputStream out ) throws IOException;
}
