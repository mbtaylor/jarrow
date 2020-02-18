package jarrow.feather;

import jarrow.fbs.Type;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

public abstract class DefaultColumnWriter implements FeatherColumnWriter {

    private final String name_;
    private final byte featherType_;
    private final long nrow_;
    private final boolean isNullable_;
    private final String userMeta_;

    protected DefaultColumnWriter( String name, byte featherType, long nrow,
                                   boolean isNullable, String userMeta ) {
        name_ = name;
        featherType_ = featherType;
        nrow_ = nrow;
        isNullable_ = isNullable;
        userMeta_ = userMeta;
    }

    public String getName() {
        return name_;
    }

    public byte getFeatherType() {
        return featherType_;
    }

    public String getUserMetadata() {
        return userMeta_;
    }

    public ColStat writeColumnBytes( OutputStream out ) throws IOException {
        long nNull = 0;
        final long maskBytes;
        if ( isNullable_ ) {
            int mask = 0;
            int ibit = 0;
            for ( long ir = 0; ir < nrow_; ir++ ) {
                if ( isNull( ir ) ) {
                    nNull++;
                    mask |= 1 << ibit++;
                }
                if ( ibit == 8 ) {
                    out.write( mask );
                    ibit = 0;
                    mask = 0;
                }
            }
            if ( ibit > 0 ) {
                out.write( mask );
            }
            long mb = ( nrow_ + 7 ) / 8;
            maskBytes = mb + BufUtils.align8( out, mb );
        }
        else {
            maskBytes = 0;
        }
        long dataBytes = writeDataBytes( out );
        dataBytes += BufUtils.align8( out, dataBytes );
        boolean hasNull = nNull > 0;
        final long byteCount = hasNull ? maskBytes + dataBytes : dataBytes;
        final long dataOffset = hasNull ? 0 : maskBytes;
        final long nullCount = nNull;
        return new ColStat() {
            public long getByteCount() {
                return byteCount;
            }
            public long getDataOffset() {
                return dataOffset;
            }
            public long getNullCount() {
                return nullCount;
            }
        };
    }

    // Only called if isNullable returns true
    public abstract boolean isNull( long irow );

    // Excluding any mask.  Doesn't need to be aligned.
    // @return   number of bytes written
    public abstract long writeDataBytes( OutputStream out ) throws IOException;

    public static FeatherColumnWriter
            createDoubleWriter( String name, final double[] data,
                                String userMeta ) {
        return new PrimitiveArrayWriter( name, Type.DOUBLE, userMeta, 8,
                                         data.length ) {
            protected void writeData( OutputStream out ) throws IOException {
                for ( int i = 0; i < nrow_; i++ ) {
                    BufUtils.writeLittleEndianDouble( out, data[ i ] );
                }
            }
        };
    }

    public static FeatherColumnWriter
            createFloatWriter( String name, final float[] data,
                               String userMeta ) {
        return new PrimitiveArrayWriter( name, Type.FLOAT, userMeta, 4,
                                         data.length ) {
            protected void writeData( OutputStream out ) throws IOException {
                for ( int i = 0; i < nrow_; i++ ) {
                    BufUtils.writeLittleEndianFloat( out, data[ i ] );
                }
            }
        };
    }

    public static FeatherColumnWriter
            createLongWriter( String name, final long[] data,
                              String userMeta ) {
        return new PrimitiveArrayWriter( name, Type.INT64, userMeta, 8,
                                         data.length ) {
            protected void writeData( OutputStream out ) throws IOException {
                for ( int i = 0; i < nrow_; i++ ) {
                    BufUtils.writeLittleEndianLong( out, data[ i ] );
                }
            }
        };
    }

    public static FeatherColumnWriter
            createIntWriter( String name, final int[] data, String userMeta ) {
        return new PrimitiveArrayWriter( name, Type.INT32, userMeta, 4,
                                         data.length ) {
            protected void writeData( OutputStream out ) throws IOException {
                for ( int i = 0; i < nrow_; i++ ) {
                    BufUtils.writeLittleEndianInt( out, data[ i ] );
                }
            }
        };
    }

    public static FeatherColumnWriter
            createShortWriter( String name, final short[] data,
                               String userMeta ) {
        return new PrimitiveArrayWriter( name, Type.INT16, userMeta, 2,
                                         data.length ) {
            protected void writeData( OutputStream out ) throws IOException {
                for ( int i = 0; i < nrow_; i++ ) {
                    BufUtils.writeLittleEndianShort( out, data[ i ] );
                }
            }
        };
    }

    public static FeatherColumnWriter
            createByteWriter( String name, final byte[] data,
                              String userMeta ) {
        return new PrimitiveArrayWriter( name, Type.INT8, userMeta, 1,
                                         data.length ) {
            protected void writeData( OutputStream out ) throws IOException {
                out.write( data );
            }
        };
    }

    public static FeatherColumnWriter
            createStringWriter( String name, final String[] data,
                                String userMeta, boolean isNullable ) {
        final Charset UTF8 = Charset.forName( "UTF-8" );
        return new VariableLengthWriter( name, Type.UTF8, data.length,
                                         isNullable, userMeta ) {
            public boolean isNull( long irow ) {
                return data[ BufUtils.longToInt( irow ) ] == null;
            }
            public int getByteSize( long irow ) {
                String str = data[ BufUtils.longToInt( irow ) ];
                return str == null ? 0 : str.getBytes( UTF8 ).length;
            }
            public void writeItem( long irow, OutputStream out )
                    throws IOException {
                String str = data[ BufUtils.longToInt( irow ) ];
                if ( str != null ) {
                    out.write( str.getBytes( UTF8 ) );
                }
            }
        };
    }

    public static abstract class PrimitiveArrayWriter
            extends DefaultColumnWriter {
        final int size_;
        final long nrow_;
        PrimitiveArrayWriter( String name, byte type, String userMeta,
                              int size, long nrow ) {
            super( name, type, nrow, false, userMeta );
            size_ = size;
            nrow_ = nrow;
        }
        public boolean isNull( long irow ) {
            return false;
        }
        public long writeDataBytes( OutputStream out ) throws IOException {
            writeData( out );
            return size_ * nrow_;
        }
        protected abstract void writeData( OutputStream out )
                throws IOException;
    }

    private static abstract class VariableLengthWriter
            extends DefaultColumnWriter {
        private final long nrow_;
        VariableLengthWriter( String name, byte type, long nrow,
                              boolean isNullable, String userMeta ) {
            super( name, type, nrow, isNullable, userMeta );
            nrow_ = nrow;
        }

        abstract int getByteSize( long irow );
        abstract void writeItem( long irow, OutputStream out )
                throws IOException;

        public long writeDataBytes( OutputStream out ) throws IOException {
            int ioff = 0;
            for ( int ir = 0; ir < nrow_; ir++ ) {
                BufUtils.writeLittleEndianInt( out, ioff );
                ioff += getByteSize( ir );
            }
            BufUtils.writeLittleEndianInt( out, ioff );
            long nbyteIndex = 4 * ( nrow_ + 1 );
            nbyteIndex += BufUtils.align8( out, nbyteIndex );
            long nbyteData = ioff;
            for ( int ir = 0; ir < nrow_; ir++ ) {
                writeItem( ir, out );
            }
            nbyteData += BufUtils.align8( out, nbyteData );
            return nbyteIndex + nbyteData;
        }
    }

    private static class NoNullColStat implements ColStat {
        private final long byteCount_;
        NoNullColStat( long byteCount ) {
            byteCount_ = byteCount;
        }
        public long getByteCount() {
            return byteCount_;
        }
        public long getDataOffset() {
            return 0;
        }
        public long getNullCount() {
            return 0;
        }
    }
}
