package jarrow.feather;

import jarrow.fbs.Column;
import java.io.IOException;
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
        return decoder_
              .createReader( mapper_.mapBuffer()
                            .order( ByteOrder.LITTLE_ENDIAN ) );
    }

    @Override
    public String toString() {
        return name_;
    }
}
