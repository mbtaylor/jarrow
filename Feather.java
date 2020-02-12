
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class Feather {

    private static final int MAGIC = ( 'F' & 0xff ) <<  0
                                   | ( 'E' & 0xff ) <<  8
                                   | ( 'A' & 0xff ) << 16
                                   | ( '1' & 0xff ) << 24;

    public Feather( FeatherMeta meta ) {
    }

    static int readLittleEndianInt( RandomAccessFile raf ) throws IOException {
        return ( raf.read() & 0xff ) <<  0
             | ( raf.read() & 0xff ) <<  8
             | ( raf.read() & 0xff ) << 16
             | ( raf.read() & 0xff ) << 24;
    }

    public static Feather fromFile( File file ) throws IOException {
        RandomAccessFile raf = new RandomAccessFile( file, "r" );
        long leng = raf.length();
        int magic1 = readLittleEndianInt( raf );
        if ( magic1 != MAGIC ) {
            throw new IOException( "Not FEA1 magic number at file start" );
        }
        raf.seek( leng - 8 );
        int metaLeng = readLittleEndianInt( raf );
        int magic2 = readLittleEndianInt( raf );
        if ( magic2 != MAGIC ) {
            throw new IOException( "Not FEA1 magic number at file start" );
        }
        long metaStart = leng - 8 - metaLeng;
        byte[] metabuf = new byte[ metaLeng ];
        raf.seek( 0 );
        raf.readFully( metabuf );
        FeatherMeta meta = FeatherMeta.fromBytes( metabuf );
        return new Feather( meta );
    }
}
