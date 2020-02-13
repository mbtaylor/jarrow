package jarrow.feather;

import jarrow.fbs.CTable;
import jarrow.fbs.Column;
import jarrow.fbs.PrimitiveArray;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FeatherTable {

    private final CTable ctable_;
    private final FileChannel channel_;
    private final String name_;
    private final int ncol_;
    private final long nrow_;
    private final FeatherColumn[] columns_;

    public static final int MAGIC = ( 'F' & 0xff ) <<  0
                                  | ( 'E' & 0xff ) <<  8
                                  | ( 'A' & 0xff ) << 16
                                  | ( '1' & 0xff ) << 24;

    public FeatherTable( CTable ctable, FileChannel channel, String name ) {
        ctable_ = ctable;
        channel_ = channel;
        name_ = name;
        ncol_ = ctable.columnsLength();
        nrow_ = ctable.numRows();
        columns_ = new FeatherColumn[ ncol_ ];
        for ( int ic = 0; ic < ncol_; ic++ ) {
            Column col = ctable.columns( ic );
            String cname = col.name();
            PrimitiveArray parr = col.values();
            byte type = parr.type();
            long nNull = parr.nullCount();
            String userMeta = col.userMetadata();
            Decoder<?> decoder =
                Decoder.createDecoder( parr.type(), nNull > 0 );
            BufMapper mapper =
                new BufMapper( channel, parr.offset(), parr.totalBytes() );
            columns_[ ic ] =
                new FeatherColumn( cname, nrow_, mapper, decoder,
                                   nNull, userMeta );
        }
    }

    public int getColumnCount() {
        return ncol_;
    }

    public long getRowCount() {
        return nrow_;
    }

    public FeatherColumn getColumn( int icol ) {
        return columns_[ icol ];
    }

    public String getDescription() {
        return ctable_.description();
    }

    public String getMetadata() {
        return ctable_.metadata();
    }

    public int getFeatherVersion() {
        return ctable_.version();
    }

    public void close() throws IOException {
        channel_.close();
    }

    @Override
    public String toString() {
        StringBuffer sbuf = new StringBuffer()
           .append( name_ )
           .append( " (" )
           .append( ncol_ )
           .append( "x" )
           .append( nrow_ )
           .append( ")" )
           .append( ": " );
        for ( int ic = 0; ic < ncol_; ic++ ) {
            if ( ic > 0 ) {
                sbuf.append( ", " );
            }
            sbuf.append( getColumn( ic ) );
        }
        return sbuf.toString();
    }

    static int readLittleEndianInt( RandomAccessFile raf ) throws IOException {
        return ( raf.read() & 0xff ) <<  0
             | ( raf.read() & 0xff ) <<  8
             | ( raf.read() & 0xff ) << 16
             | ( raf.read() & 0xff ) << 24;
    }

    public static FeatherTable fromFile( File file ) throws IOException {
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
        raf.seek( metaStart );
        raf.readFully( metabuf );
        CTable ctable = CTable.getRootAsCTable( ByteBuffer.wrap( metabuf ) );
        return new FeatherTable( ctable, raf.getChannel(), file.toString() );
    }

    public static void main( String[] args ) throws IOException {
        FeatherTable ft = FeatherTable.fromFile( new File( args[ 0 ] ) );
        System.out.println( ft );
        int ncol = ft.getColumnCount();
        Reader<?>[] rdrs = new Reader<?>[ ncol ];
        System.out.print( "#" );
        for ( int ic = 0; ic < ncol; ic++ ) {
            FeatherColumn fcol = ft.getColumn( ic );
            rdrs[ ic ] = fcol.createReader();
            System.out.print( "\t" + fcol.getName() );
        }
        System.out.println();
        long nrow = ft.getRowCount();
        for ( long ir = 0; ir < nrow; ir++ ) {
            for ( int ic = 0; ic < ncol; ic++ ) {
                System.out.print( "\t" + rdrs[ ic ].getObject( ir ) );
            }
            System.out.println();
        }
    }
}
