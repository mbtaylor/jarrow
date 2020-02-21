package jarrow.feather;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class BufMapper {

    private final FileChannel channel_;
    private final long start_;
    private final long length_;
    private static final FileChannel.MapMode RO_MODE =
        FileChannel.MapMode.READ_ONLY;

    public BufMapper( FileChannel channel, long start, long length ) {
        channel_ = channel;
        start_ = start;
        length_ = length;
    }

    public Buf mapBuffer() throws IOException {
        return mapBuffer( 0 );
    }

    public Buf mapBuffer( long offset ) throws IOException {
        long off = start_ + offset;
        long leng = length_ - offset;
        BufferAccess access =
              leng < Integer.MAX_VALUE
            ? new SimpleAccess( channel_, off, leng )
            : new MultiAccess( channel_, off, leng );
        return new Buf( access );
    }

    private static class SimpleAccess implements BufferAccess {
        private final ByteBuffer bbuf_;

        public SimpleAccess( FileChannel channel, long offset, long leng )
                throws IOException {
            bbuf_ = channel.map( RO_MODE, offset, leng );
        }
    
        public byte get( long ix ) {
            return bbuf_.get( longToInt( ix ) );
        } 

        private static int longToInt( long ix ) {
            return (int) ix;
        }
    }

    private static class MultiAccess implements BufferAccess {
        private final FileChannel channel_;
        private final long offset_;
        private final long leng_;
        private ByteBuffer[] bbufs_;
        private static final int POW2 = 30;
        private static final long SIZE = 1L << POW2;
        private static final int MASK = (int) SIZE - 1;
        public MultiAccess( FileChannel channel, long offset, long leng ) {
            channel_ = channel;
            offset_ = offset;
            leng_ = leng;
            bbufs_ = new ByteBuffer[ bankIndex( leng ) + 1 ];
        }
        public byte get( long ix ) {
            return getByteBuffer( bankIndex( ix ) ).get( bankOffset( ix ) );
        }
        public ByteBuffer getByteBuffer( int ibuf ) {
            ByteBuffer bbuf = bbufs_[ ibuf ];
            if ( bbuf != null ) {
                return bbuf;
            }
            else {
                long boff1 = offset_ + ibuf * SIZE; 
                long leng1 = Math.min( SIZE, leng_ - ibuf * SIZE );
                ByteBuffer bbuf1;
                try {
                    bbuf1 = channel_.map( RO_MODE, boff1, leng1 );
                }
                catch ( IOException e ) {
                    throw new RuntimeException( "File mapping failure: " + e,
                                                e );
                }
                bbufs_[ ibuf ] = bbuf1;
                return bbuf1;
            }
        }
        private int bankIndex( long ix ) {
            return (int) ( ix >> POW2 );
        }
        private int bankOffset( long ix ) {
            return (int) ix & MASK;
        }
    }
}
