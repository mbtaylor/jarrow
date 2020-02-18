package jarrow.feather;

import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class BufUtils {

    public static final Charset UTF8 = Charset.forName( "UTF-8" );

    private BufUtils() {
    }

    public static int longToInt( long index ) {
        int ix = (int) index;
        if ( ix == index ) {
            return ix;
        }
        else {
            throw new RuntimeException( "Integer overflow!" );
        }
    }

    public static boolean isBitSet( ByteBuffer bbuf, long ix ) {
        return ( bbuf.get( longToInt( ix / 8 ) )
               & ( 1 << ((int) ix) % 8 ) ) != 0;
    }

    public static int readLittleEndianInt( RandomAccessFile raf )
            throws IOException {
        return ( raf.read() & 0xff ) <<  0
             | ( raf.read() & 0xff ) <<  8
             | ( raf.read() & 0xff ) << 16
             | ( raf.read() & 0xff ) << 24;
    }


    public static void writeLittleEndianLong( OutputStream out, long l )
            throws IOException {
        out.write( ( (int) ( l >>  0 ) ) & 0xff );
        out.write( ( (int) ( l >>  8 ) ) & 0xff );
        out.write( ( (int) ( l >> 16 ) ) & 0xff );
        out.write( ( (int) ( l >> 24 ) ) & 0xff );
        out.write( ( (int) ( l >> 32 ) ) & 0xff );
        out.write( ( (int) ( l >> 40 ) ) & 0xff );
        out.write( ( (int) ( l >> 48 ) ) & 0xff );
        out.write( ( (int) ( l >> 56 ) ) & 0xff );
    }

    public static void writeLittleEndianInt( OutputStream out, int i )
            throws IOException {
        out.write( ( i >>  0 ) & 0xff );
        out.write( ( i >>  8 ) & 0xff );
        out.write( ( i >> 16 ) & 0xff );
        out.write( ( i >> 24 ) & 0xff );
    }

    public static void writeLittleEndianShort( OutputStream out, short s )
            throws IOException {
        out.write( ( s >> 0 ) & 0xff );
        out.write( ( s >> 8 ) & 0xff );
    }

    public static void writeLittleEndianDouble( OutputStream out, double d )
            throws IOException {
        writeLittleEndianLong( out, Double.doubleToLongBits( d ) );
    }

    public static void writeLittleEndianFloat( OutputStream out, float f )
            throws IOException {
        writeLittleEndianInt( out, Float.floatToIntBits( f ) );
    }

    public static int align8( OutputStream out, long nb ) throws IOException {
        int over = (int) ( nb % 8 );
        int pad;
        if ( over > 0 ) {
            pad = 8 - over;
            for ( int i = 0; i < pad; i++ ) {
                out.write( 0 );
            }
        }
        else {
            pad = 0;
        }
        return pad;
    }
}
