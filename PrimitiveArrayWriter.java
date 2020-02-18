package jarrow.feather;

import jarrow.fbs.Type;
import java.io.IOException;
import java.io.OutputStream;

public abstract class PrimitiveArrayWriter extends AbstractColumnWriter {

    private final long nbyte_;

    public PrimitiveArrayWriter( String name, byte type, String userMeta,
                                 int size, long nrow ) {
        super( name, type, nrow, false, userMeta );
        nbyte_ = size * nrow;
    }

    abstract void writeData( OutputStream out ) throws IOException;

    /**
     * Returns false, but may be overridden.
     */
    public boolean isNull( long irow ) {
        return false;
    }

    public long writeDataBytes( OutputStream out ) throws IOException {
        writeData( out );
        return nbyte_;
    }

    public static FeatherColumnWriter
            createDoubleWriter( String name, final double[] data,
                                String userMeta ) {
        final int nrow = data.length;
        return new PrimitiveArrayWriter( name, Type.DOUBLE, userMeta, 8, nrow) {
            protected void writeData( OutputStream out ) throws IOException {
                for ( int i = 0; i < nrow; i++ ) {
                    BufUtils.writeLittleEndianDouble( out, data[ i ] );
                }
            }
        };
    }

    public static FeatherColumnWriter
            createFloatWriter( String name, final float[] data,
                               String userMeta ) {
        final int nrow = data.length;
        return new PrimitiveArrayWriter( name, Type.FLOAT, userMeta, 4, nrow ) {
       
            protected void writeData( OutputStream out ) throws IOException {
                for ( int i = 0; i < nrow; i++ ) {
                    BufUtils.writeLittleEndianFloat( out, data[ i ] );
                }
            }
        };
    }

    public static FeatherColumnWriter
            createLongWriter( String name, final long[] data,
                              String userMeta ) {
        final int nrow = data.length;
        return new PrimitiveArrayWriter( name, Type.INT64, userMeta, 8, nrow ) {
            protected void writeData( OutputStream out ) throws IOException {
                for ( int i = 0; i < nrow; i++ ) {
                    BufUtils.writeLittleEndianLong( out, data[ i ] );
                }
            }
        };
    }

    public static FeatherColumnWriter
            createIntWriter( String name, final int[] data, String userMeta ) {
        final int nrow = data.length;
        return new PrimitiveArrayWriter( name, Type.INT32, userMeta, 4, nrow ) {
            protected void writeData( OutputStream out ) throws IOException {
                for ( int i = 0; i < nrow; i++ ) {
                    BufUtils.writeLittleEndianInt( out, data[ i ] );
                }
            }
        };
    }

    public static FeatherColumnWriter
            createShortWriter( String name, final short[] data,
                               String userMeta ) {
        final int nrow = data.length;
        return new PrimitiveArrayWriter( name, Type.INT16, userMeta, 2, nrow ) {
            protected void writeData( OutputStream out ) throws IOException {
                for ( int i = 0; i < nrow; i++ ) {
                    BufUtils.writeLittleEndianShort( out, data[ i ] );
                }
            }
        };
    }

    public static FeatherColumnWriter
            createByteWriter( String name, final byte[] data,
                              String userMeta ) {
        final int nrow = data.length;
        return new PrimitiveArrayWriter( name, Type.INT8, userMeta, 1, nrow ) {
            protected void writeData( OutputStream out ) throws IOException {
                out.write( data );
            }
        };
    }
}
