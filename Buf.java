package jarrow.feather;

public class Buf {

    private final BufferAccess access_;

    public Buf( BufferAccess access ) {
        access_ = access;
    }

    public byte get( long ix ) {
        return access_.get( ix );
    }

    // note these are little-endian

    public short getShort( long ix ) {
        return (short)
               ( ( ( get( ix + 0 ) & 0xff ) <<  0 )
               | ( ( get( ix + 1 ) & 0xff ) <<  8 ) );
    }

    public int getInt( long ix ) {
        return ( ( get( ix + 0 ) & 0xff ) <<  0 )
             | ( ( get( ix + 1 ) & 0xff ) <<  8 )
             | ( ( get( ix + 2 ) & 0xff ) << 16 )
             | ( ( get( ix + 3 ) & 0xff ) << 24 );
    }

    public long getLong( long ix ) {
        return ( ( get( ix + 0 ) & 0xffL ) <<  0 )
             | ( ( get( ix + 1 ) & 0xffL ) <<  8 )
             | ( ( get( ix + 2 ) & 0xffL ) << 16 )
             | ( ( get( ix + 3 ) & 0xffL ) << 24 )
             | ( ( get( ix + 4 ) & 0xffL ) << 32 )
             | ( ( get( ix + 5 ) & 0xffL ) << 40 )
             | ( ( get( ix + 6 ) & 0xffL ) << 48 )
             | ( ( get( ix + 7 ) & 0xffL ) << 56 );
    }

    public float getFloat( long ix ) {
        return Float.intBitsToFloat( getInt( ix ) );
    }

    public double getDouble( long ix ) {
        return Double.longBitsToDouble( getLong( ix ) );
    }

    public void get( long ix, byte[] dbytes ) {
        int nb = dbytes.length;
        for ( int ib = 0; ib < nb; ib++ ) {
            dbytes[ ib ] = get( ix + ib );
        }
    }

    public boolean isBitSet( long ix ) {
        return ( get( ix / 8 ) & ( 1 << ((int) ix) % 8 ) )
             != 0;
    }
}
