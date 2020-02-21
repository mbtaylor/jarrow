package jarrow.feather;

import jarrow.fbs.Column;
import java.io.IOException;

public class FeatherColumn {

    private final String name_;
    private final long nrow_;
    private final BufMapper mapper_;
    private final Decoder<?> decoder_;
    private final long nNull_;
    private final String userMeta_;
    private final byte featherType_;

    public FeatherColumn( String name, long nrow, BufMapper mapper,
                          Decoder<?> decoder, long nNull, String userMeta,
                          byte featherType ) {
        name_ = name;
        nrow_ = nrow;
        mapper_ = mapper;
        decoder_ = decoder;
        nNull_ = nNull;
        userMeta_ = userMeta;
        featherType_ = featherType;
    }

    public String getName() {
        return name_;
    }

    public Class<?> getValueClass() {
        return decoder_.getValueClass();
    }

    public byte getFeatherType() {
        return featherType_;
    }

    public String getFeatherTypeName() {
        return decoder_.toString();
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
            return decoder_.createReader( mapper_.mapBuffer(), nrow_ );
        }
        else {
            // The Feather docs say this is byte aligned, but it looks like
            // it's aligned on 64-bit boundaries.
            long dataOffset = ( ( nrow_ + 63 ) / 64 ) * 8;
            Buf maskBuf = mapper_.mapBuffer();
            Buf dataBuf = mapper_.mapBuffer( dataOffset );
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
                                                   final Buf maskBuf ) {
        return new Reader<T>() {
            private boolean isMask( long ix ) {
                return maskBuf.isBitSet( ix );
            }
            public T getObject( long ix ) {
                return isMask( ix ) ? basicReader.getObject( ix ) : null;
            }
            public byte getByte( long ix ) {
                return isMask( ix ) ? basicReader.getByte( ix ) : null;
            }
            public short getShort( long ix ) {
                return isMask( ix ) ? basicReader.getShort( ix ) : null;
            }
            public int getInt( long ix ) {
                return isMask( ix ) ? basicReader.getInt( ix ) : null;
            }
            public long getLong( long ix ) {
                return isMask( ix ) ? basicReader.getLong( ix ) : null;
            }
            public float getFloat( long ix ) {
                return isMask( ix ) ? basicReader.getFloat( ix ) : Float.NaN;
            }
            public double getDouble( long ix ) {
                return isMask( ix ) ? basicReader.getDouble( ix ) : Double.NaN;
            }
            public Class<T> getValueClass() {
                return basicReader.getValueClass();
            }
        };
    }
}
