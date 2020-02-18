package jarrow.feather;

import jarrow.fbs.Column;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class FeatherColumn {

    private final String name_;
    private final long nrow_;
    private final BufMapper mapper_;
    private final Decoder<?> decoder_;
    private final long nNull_;
    private final String userMeta_;

    public FeatherColumn( String name, long nrow, BufMapper mapper,
                          Decoder<?> decoder, long nNull, String userMeta ) {
        name_ = name;
        nrow_ = nrow;
        mapper_ = mapper;
        decoder_ = decoder;
        nNull_ = nNull;
        userMeta_ = userMeta;
    }

    public String getName() {
        return name_;
    }

    public long getRowCount() {
        return nrow_;
    }

    public String getUserMeta() {
        return userMeta_;
    }

    public long getNullCount() {
        return nNull_;
    }

    public Reader<?> createReader() throws IOException {
        if ( nNull_ == 0 ) {
            ByteBuffer bbuf =
                mapper_.mapBuffer().order( ByteOrder.LITTLE_ENDIAN );
            return decoder_.createReader( bbuf, nrow_ );
        }
        else {
            // The Feather docs say this is byte aligned, but it looks like
            // it's aligned on 64-bit boundaries.
            int dataOffset = BufUtils.longToInt( ( ( nrow_ + 63 ) / 64 ) * 8 );
            ByteBuffer maskBuf =
                mapper_.mapBuffer().order( ByteOrder.LITTLE_ENDIAN );
            ByteBuffer dataBuf =
                mapper_.mapBuffer( dataOffset )
                       .order( ByteOrder.LITTLE_ENDIAN );
            return createMaskReader( decoder_.createReader( dataBuf, nrow_ ),
                                     maskBuf );
        }
    }

    @Override
    public String toString() {
        StringBuffer sbuf = new StringBuffer()
            .append( name_ ) 
            .append( "(" )
            .append( decoder_ );
        if ( nNull_ > 0 ) {
            sbuf.append( ",nulls=" )
                .append( nNull_ );
        }
        if ( userMeta_ != null && userMeta_.trim().length() > 0 ) {
            sbuf.append( ":" )
                .append( '"' )
                .append( userMeta_ )
                .append( '"' );
        }
        sbuf.append( ")" );
        return sbuf.toString();
    }

    private static <T> Reader<T> createMaskReader( final Reader<T> basicReader,
                                                   final ByteBuffer maskBuf ) {
        return new Reader<T>() {
            private boolean isNull( long ix ) {
                return BufUtils.isBitSet( maskBuf, ix );
            }
            public T getObject( long ix ) {
                return isNull( ix ) ? basicReader.getObject( ix ) : null;
            }
            public byte getByte( long ix ) {
                return isNull( ix ) ? basicReader.getByte( ix ) : null;
            }
            public short getShort( long ix ) {
                return isNull( ix ) ? basicReader.getShort( ix ) : null;
            }
            public int getInt( long ix ) {
                return isNull( ix ) ? basicReader.getInt( ix ) : null;
            }
            public long getLong( long ix ) {
                return isNull( ix ) ? basicReader.getLong( ix ) : null;
            }
            public float getFloat( long ix ) {
                return isNull( ix ) ? basicReader.getFloat( ix ) : Float.NaN;
            }
            public double getDouble( long ix ) {
                return isNull( ix ) ? basicReader.getDouble( ix ) : Double.NaN;
            }
            public Class<T> getValueClass() {
                return basicReader.getValueClass();
            }
        };
    }
}
