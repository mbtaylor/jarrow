package uk.ac.starlink.feather;

import jarrow.fbs.Type;
import jarrow.feather.FeatherColumn;
import jarrow.feather.FeatherTable;
import jarrow.feather.Reader;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.json.JSONException;
import org.json.JSONObject;
import uk.ac.starlink.table.AbstractStarTable;
import uk.ac.starlink.table.ColumnInfo;
import uk.ac.starlink.table.DescribedValue;
import uk.ac.starlink.table.RowSequence;
import uk.ac.starlink.table.Tables;

public class FeatherStarTable extends AbstractStarTable {

    private final FeatherTable ftable_;
    private final int ncol_;
    private final long nrow_;
    private final String name_;
    private final FeatherColumn[] fcols_;
    private final ColumnInfo[] colInfos_;
    private final RowReader randomReader_;
    private static final String UCD_KEY = "ucd";
    private static final String UTYPE_KEY = "utype";
    private static final String UNIT_KEY = "unit";
    private static final String DESCRIPTION_KEY = "description";
    private static final String META_KEY = "meta";

    public FeatherStarTable( FeatherTable ftable ) {
        ftable_ = ftable;
        ncol_ = ftable.getColumnCount();
        nrow_ = ftable.getRowCount();
        name_ = ftable.getDescription();
        fcols_ = new FeatherColumn[ ncol_ ];
        colInfos_ = new ColumnInfo[ ncol_ ];
        for ( int icol = 0; icol < ncol_; icol++ ) {
            fcols_[ icol ] = ftable.getColumn( icol );
            colInfos_[ icol ] = getColumnInfo( fcols_[ icol ] );
        }
        randomReader_ = new RowReader();
    }

    public int getColumnCount() {
        return ncol_;
    }

    public long getRowCount() {
        return nrow_;
    }

    public boolean isRandom() {
        return true;
    }

    public ColumnInfo getColumnInfo( int icol ) {
        return colInfos_[ icol ];
    }

    public synchronized Object getCell( long irow, int icol )
            throws IOException {
        return randomReader_.getCell( irow, icol );
    }

    public synchronized Object getRow( long irow, int icol )
            throws IOException {
        return randomReader_.getRow( irow );
    }

    public RowSequence getRowSequence() {
        final RowReader rowReader = new RowReader();
        return new RowSequence() {
            long irow_ = -1;
            public boolean next() {
                if ( irow_ < nrow_ - 1 ) {
                    irow_++;
                    return true;
                }
                else {
                    return false;
                }
            }
            public Object getCell( int icol ) throws IOException {
                return rowReader.getCell( irow_, icol );
            }
            public Object[] getRow() throws IOException {
                return rowReader.getRow( irow_ );
            }
            public void close() {
            }
        };
    }

    private static ColumnInfo getColumnInfo( FeatherColumn fcol ) {
        Class<?> clazz = fcol.getValueClass();
        ColumnInfo info = new ColumnInfo( fcol.getName(), clazz, null );
        info.setNullable( fcol.getNullCount() > 0 );
        Map<String,String> metaMap = getMetaMap( fcol.getUserMeta() );
        for ( Map.Entry<String,String> entry : metaMap.entrySet() ) {
            String key = entry.getKey();
            String value = entry.getValue();
            if ( key.equals( UCD_KEY ) ) {
                info.setUCD( value );
            }
            if ( key.equals( UTYPE_KEY ) ) {
                info.setUtype( value );
            }
            if ( key.equals( UNIT_KEY ) ) {
                info.setUnitString( value );
            }
            if ( key.equals( DESCRIPTION_KEY ) ) {
                info.setDescription( value );
            }
        }
        if ( fcol.getFeatherType() == Type.UINT8 &&
             clazz.equals( Short.class ) ) {
            info.setAuxDatum( new DescribedValue( Tables.UBYTE_FLAG_INFO,
                                                  Boolean.TRUE ) );
        }
        return info;
    }

    private static Map<String,String> getMetaMap( String userMeta ) {
        Map<String,String> map = new LinkedHashMap<>();
        if ( userMeta != null && userMeta.trim().length() > 0 ) {
            try {
                JSONObject json = new JSONObject( userMeta );
                for ( String key : json.keySet() ) {
                    String lkey = key.toLowerCase();
                    Object value = json.get( key );
                    if ( key.equals( "ucd" ) ) {
                        map.put( UCD_KEY, value.toString() );
                    }
                    else if ( key.equals( "utype" ) ) {
                        map.put( UTYPE_KEY, value.toString() );
                    }
                    else if ( key.equals( "unit" ) || key.equals( "units" ) ) {
                        map.put( UNIT_KEY, value.toString() );
                    }
                    else if ( key.equals( "description" ) ) {
                        map.put( DESCRIPTION_KEY, value.toString() );
                    }
                }
            }
            catch ( JSONException e ) {
                map.put( META_KEY, userMeta ); 
            }
        }
        return map;
    }

    private class RowReader {
        final Reader<?>[] rdrs_ = new Reader<?>[ ncol_ ];
        Reader<?> getReader( int icol ) throws IOException {
            Reader<?> rdr = rdrs_[ icol ];
            if ( rdr != null ) {
                return rdr;
            }
            else {
                rdrs_[ icol ] = fcols_[ icol ].createReader();
                return rdrs_[ icol ];
            }
        }
        Object getCell( long irow, int icol ) throws IOException {
            return getReader( icol ).getObject( irow );
        }
        Object[] getRow( long irow ) throws IOException {
            Object[] row = new Object[ ncol_ ];
            for ( int ic = 0; ic < ncol_; ic++ ) {
                row[ ic ] = getReader( ic ).getObject( irow );
            }
            return row;
        }
    }
}
