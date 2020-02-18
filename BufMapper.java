package jarrow.feather;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class BufMapper {

    private final FileChannel channel_;
    private final long start_;
    private final long length_;

    public BufMapper( FileChannel channel, long start, long length ) {
        channel_ = channel;
        start_ = start;
        length_ = length;
    }

    public ByteBuffer mapBuffer() throws IOException {
        return mapBuffer( 0 );
    }

    public ByteBuffer mapBuffer( long offset ) throws IOException {
        return channel_.map( FileChannel.MapMode.READ_ONLY,
                             start_ + offset, length_ - offset );
    }
}
