package uk.ac.starlink.feather;

import jarrow.fbs.Type;
import jarrow.feather.BufUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class FeatherEncoders {

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

    private FeatherEncoders() {
    }

    public static FeatherEncoder getEncoder( Class<?> clazz ) {
        if ( Double.class.equals( clazz ) ) {
            return new NumberEncoder( Type.DOUBLE, false, DOUBLE_NAN ) {
                public void writeNumber( OutputStream out, Number val )
                        throws IOException {
                    BufUtils.writeLittleEndianDouble( out, val.doubleValue() );
                }
            };
        }
        else if ( Float.class.equals( clazz ) ) {
            return new NumberEncoder( Type.FLOAT, false, FLOAT_NAN ) {
                public void writeNumber( OutputStream out, Number val )
                        throws IOException {
                    BufUtils.writeLittleEndianFloat( out, val.floatValue() );
                }
            };
        }
        else if ( Long.class.equals( clazz ) ) {
            return new NumberEncoder( Type.INT64, true, new byte[ 8 ] ) {
                public void writeNumber( OutputStream out, Number val )
                        throws IOException {
                    BufUtils.writeLittleEndianLong( out, val.longValue() );
                }
            };
        }
        else if ( Integer.class.equals( clazz ) ) {
            return new NumberEncoder( Type.INT32, true, new byte[ 4 ] ) {
                public void writeNumber( OutputStream out, Number val )
                        throws IOException {
                    BufUtils.writeLittleEndianInt( out, val.intValue() );
                }
            };
        }
        else if ( Short.class.equals( clazz ) ) {
            return new NumberEncoder( Type.INT16, true, new byte[ 2 ] ) {
                public void writeNumber( OutputStream out, Number val )
                        throws IOException {
                    BufUtils.writeLittleEndianShort( out, val.shortValue() );
                }
            };
        }
        else if ( Byte.class.equals( clazz ) ) {
            return new NumberEncoder( Type.INT8, true, new byte[ 1 ] ) {
                public void writeNumber( OutputStream out, Number val )
                        throws IOException {
                    out.write( val.byteValue() & 0xff );
                }
            };
        }
        else if ( Boolean.class.equals( clazz ) ) {
            return new BooleanEncoder( true );
        }
        else if ( String.class.equals( clazz ) ) {
            return new VariableLengthEncoder( Type.UTF8, false ) {
                public int getByteSize( Object value ) {
                    return value instanceof String
                         ? BufUtils.utf8Length( ((String) value) )
                         : 0;
                }
                public int writeBytes( OutputStream out, Object value )
                        throws IOException {
                    if ( value instanceof String ) {
                        byte[] bytes =
                            ((String) value).getBytes( BufUtils.UTF8 );
                        out.write( bytes );
                        return bytes.length;
                    }
                    else {
                        return 0;
                    }
                }
            };
        }
        else if ( byte[].class.equals( clazz ) ) {
            return new VariableLengthEncoder( Type.BINARY, true ) {
                public int getByteSize( Object value ) {
                    return value instanceof byte[]
                         ? ((byte[]) value).length
                         : 0;
                }
                public int writeBytes( OutputStream out, Object value )
                        throws IOException {
                    if ( value instanceof byte[] ) {
                        byte[] bytes = (byte[]) value;
                        out.write( bytes );
                        return bytes.length;
                    }
                    else {
                        return 0;
                    }
                }
            };
        }
        else {
            return null;
        }
    }

    public static FeatherEncoder getUbyteEncoder() {
        return new NumberEncoder( Type.UINT8, true, new byte[ 1 ] ) {
            public void writeNumber( OutputStream out, Number val )
                    throws IOException {
                out.write( val.shortValue() );
            }
        };
    }

    private static abstract class NumberEncoder implements FeatherEncoder {

        final byte featherType_;
        final boolean isNullable_;
        final byte[] blank_;
        final int nbyte_;
        
        NumberEncoder( byte featherType, boolean isNullable, byte[] blank ) {
            featherType_ = featherType;
            isNullable_ = isNullable;
            blank_ = blank;
            nbyte_ = blank.length;
        }

        /**
         * @param  value  not null
         */
        abstract void writeNumber( OutputStream out, Number value )
            throws IOException;

        public byte getFeatherType() {
            return featherType_;
        }

        public boolean isNullable() {
            return isNullable_;
        }

        public boolean isVariableLength() {
            return false;
        }

        public boolean isNull( Object value ) {
            assert isNullable_;
            return ! ( value instanceof Number );
        }

        public int getByteSize( Object value ) {
            assert false;
            return nbyte_;
        }

        public int writeBytes( OutputStream out, Object value )
                throws IOException {
            if ( value instanceof Number ) {
                writeNumber( out, (Number) value );
            }
            else {
                out.write( blank_ );
            }
            return nbyte_;
        }
    }

    private static class BooleanEncoder implements FeatherEncoder {
        final boolean isNullable_;

        BooleanEncoder( boolean isNullable ) {
            isNullable_ = isNullable;
        }

        public byte getFeatherType() {
            return Type.BOOL;
        }

        public boolean isNullable() {
            return isNullable_;
        }

        public boolean isVariableLength() {
            return false;
        }

        public boolean isNull( Object value ) {
            assert isNullable_;
            return value instanceof Boolean;
        }

        public int getByteSize( Object value ) {
            assert false;
            return 1;
        }

        public int writeBytes( OutputStream out, Object value )
                throws IOException {
            out.write( Boolean.TRUE.equals( value ) ? 1 : 0 );
            return 1;
        }
    }

    private static abstract class VariableLengthEncoder
            implements FeatherEncoder {

        final byte featherType_;
        final boolean isNullable_;

        VariableLengthEncoder( byte featherType, boolean isNullable ) {
            featherType_ = featherType;
            isNullable_ = isNullable;
        }

        public byte getFeatherType() {
            return featherType_;
        }

        public boolean isNullable() {
            return isNullable_;
        }

        public boolean isVariableLength() {
            return true;
        }

        public boolean isNull( Object value ) {
            assert isNullable_;
            return value == null;
        }
    }
}
