package uk.ac.starlink.feather;

import jarrow.fbs.feather.Type;
import jarrow.feather.BufUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import uk.ac.starlink.table.ColumnInfo;
import uk.ac.starlink.table.StarTable;
import uk.ac.starlink.table.Tables;


public class StarColumnWriters {

    private static final byte[] FLOAT_NAN;
    private static final byte[] DOUBLE_NAN;
    static {
        ByteArrayOutputStream dout = new ByteArrayOutputStream( 8 );
        ByteArrayOutputStream fout = new ByteArrayOutputStream( 4 );
        try {
            BufUtils.writeLittleEndianDouble( dout, Double.NaN );
            BufUtils.writeLittleEndianFloat( fout, Float.NaN );
        }
        catch ( IOException e ) {
            assert false;
        }
        DOUBLE_NAN = dout.toByteArray();
        FLOAT_NAN = fout.toByteArray();
        assert DOUBLE_NAN.length == 8;
        assert FLOAT_NAN.length == 4;
    }

    private StarColumnWriters() {
    }

    public static StarColumnWriter
            createColumnWriter( StarTable table, int icol ) {
        ColumnInfo info = table.getColumnInfo( icol );
        Class<?> clazz = info.getContentClass();
        if ( clazz == Double.class ) {
            return new NumberStarColumnWriter( table, icol, Type.DOUBLE, false,
                                               DOUBLE_NAN ) {
                public void writeNumber( OutputStream out, Number val )
                        throws IOException {
                    BufUtils.writeLittleEndianDouble( out, val.doubleValue() );
                }
            };
        }
        else if ( clazz == Float.class ) {
            return new NumberStarColumnWriter( table, icol, Type.FLOAT, false,
                                               FLOAT_NAN ) {
                public void writeNumber( OutputStream out, Number val )
                        throws IOException {
                    BufUtils.writeLittleEndianFloat( out, val.floatValue() );
                }
            };
        }
        else if ( clazz == Long.class ) {
            return new NumberStarColumnWriter( table, icol, Type.INT64, true,
                                               new byte[ 8 ] ) {
                public void writeNumber( OutputStream out, Number val )
                        throws IOException {
                    BufUtils.writeLittleEndianLong( out, val.longValue() );
                }
            };
        }
        else if ( clazz == Integer.class ) {
            return new NumberStarColumnWriter( table, icol, Type.INT32, true,
                                               new byte[ 4 ] ) {
                public void writeNumber( OutputStream out, Number val )
                        throws IOException {
                    BufUtils.writeLittleEndianInt( out, val.intValue() );
                }
            };
        }
        else if ( clazz == Short.class ) {
            if ( info.getAuxDatumValue( Tables.UBYTE_FLAG_INFO, Boolean.class )
                 == Boolean.TRUE ) {
                return new NumberStarColumnWriter( table, icol, Type.UINT8,true,
                                                   new byte[ 1 ] ) {
                    public void writeNumber( OutputStream out, Number val )
                            throws IOException {
                        out.write( val.shortValue() & 0xff );
                    }
                };
            }
            else {
                return new NumberStarColumnWriter( table, icol, Type.INT16,true,
                                                   new byte[ 2 ] ) {
                    public void writeNumber( OutputStream out, Number val )
                            throws IOException {
                        BufUtils.writeLittleEndianShort( out, val.shortValue());
                    }
                };
            }
        }
        else if ( clazz == Byte.class ) {
            return new NumberStarColumnWriter( table, icol, Type.INT8, true,
                                               new byte[ 1 ] ) {
                public void writeNumber( OutputStream out, Number val )
                        throws IOException {
                    out.write( val.byteValue() & 0xff );
                }
            };
        }
        else if ( clazz == Boolean.class ) {
            return new BooleanStarColumnWriter( table, icol );
        }
        else if ( clazz == String.class ) {
            VariableStarColumnWriter.PointerSize psize =
                VariableStarColumnWriter.PointerSize.I32;
            return VariableStarColumnWriter
                  .createStringWriter( table, icol, false, psize );
        }
        else if ( clazz == byte[].class ) {
            VariableStarColumnWriter.PointerSize psize =
                VariableStarColumnWriter.PointerSize.I32;
            return VariableStarColumnWriter
                  .createByteArrayWriter( table, icol, true, psize );
        }
        else {
            return null;
        }
    }
}
