package jarrow.feather;

import jarrow.fbs.CTable;
import jarrow.fbs.Column;
import jarrow.fbs.PrimitiveArray;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import com.google.flatbuffers.FlatBufferBuilder;

public class FeatherTableWriter {

    public static final int FEATHER_VERSION = 1;

    private final String description_;
    private final String tableUserMeta_;
    private final FeatherColumnWriter[] colWriters_;

    public FeatherTableWriter( String description, String tableMeta,
                               FeatherColumnWriter[] colWriters ) {
        description_ = description;
        tableUserMeta_ = tableMeta;
        colWriters_ = colWriters;
    }

    public void write( OutputStream out ) throws IOException {
        out = new BufferedOutputStream( out );
        long baseOffset = 0;
        baseOffset += writeLittleEndianInt( out, FeatherTable.MAGIC );
        baseOffset += writeLittleEndianInt( out, 0 );
        assert baseOffset % 8 == 0;
        int nc = colWriters_.length;
        ColStat[] colStats = new ColStat[ nc ];
        for ( int ic = 0; ic < nc; ic++ ) {
            colStats[ ic ] = colWriters_[ ic ].writeColumnBytes( out );
            assert colStats[ ic ].getByteCount() % 8 == 0;
        }
        byte[] metablock = createMetadataBlock( colStats, baseOffset );
        int metaSize = metablock.length;
        assert metaSize % 8 == 0;
        out.write( metablock );
        writeLittleEndianInt( out, metaSize );
        writeLittleEndianInt( out, FeatherTable.MAGIC );
        out.flush();
    }

    private byte[] createMetadataBlock( ColStat[] colStats, long baseOffset ) {
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int descriptionTag = createStringTag( builder, description_ );
        int tableUserMetaTag = createStringTag( builder, tableUserMeta_ );

        int nc = colWriters_.length;
        int[] colTags = new int[ nc ];
        long streamOffset = baseOffset;
        long nrow = 0;
        for ( int ic = 0; ic < nc; ic++ ) {
            FeatherColumnWriter colWriter = colWriters_[ ic ];
            ColStat colStat = colStats[ ic ];
            long nr = colStat.getRowCount();
            long nnull = colStat.getNullCount();
            nrow = ic == 0 ? nr : Math.min( nr, nrow );
            long internalOffset = nnull == 0
                                ? colStat.getDataOffset()
                                : 0;
            long offset = streamOffset + internalOffset;
            long nbyte = colStat.getByteCount() - internalOffset;
            streamOffset += colStat.getByteCount();
            PrimitiveArray.startPrimitiveArray( builder );
            PrimitiveArray.addType( builder, colWriter.getFeatherType() );
            PrimitiveArray.addOffset( builder, offset );
            PrimitiveArray.addLength( builder, nr );
            PrimitiveArray.addNullCount( builder, nnull );
            PrimitiveArray.addTotalBytes( builder, nbyte );
            int valuesTag = PrimitiveArray.endPrimitiveArray( builder );

            int colNameTag = createStringTag( builder, colWriter.getName() );
            int colUserMetaTag =
                createStringTag( builder, colWriter.getUserMetadata() );
            Column.startColumn( builder );
            Column.addName( builder, colNameTag );
            Column.addUserMetadata( builder, colUserMetaTag );
            Column.addValues( builder, valuesTag );
            colTags[ ic ] = Column.endColumn( builder );
        }
        int columnsTag = CTable.createColumnsVector( builder, colTags );
 
        CTable.startCTable( builder );
        CTable.addVersion( builder, FEATHER_VERSION );
        CTable.addNumRows( builder, nrow );
        CTable.addDescription( builder, descriptionTag );
        CTable.addMetadata( builder, tableUserMetaTag );
        CTable.addColumns( builder, columnsTag );
        int ctableTag = CTable.endCTable( builder );
        CTable.finishCTableBuffer( builder, ctableTag );

        return builder.sizedByteArray();
    }

    private static int createStringTag( FlatBufferBuilder builder, String s ) {
        return s == null ? 0 : builder.createString( s );
    }

    private static int writeLittleEndianInt( OutputStream out, int ivalue )
            throws IOException {
        BufUtils.writeLittleEndianInt( out, ivalue );
        return 4;
    }

    public static void main( String[] args ) throws IOException {
        int nrow = args.length > 0 ? Integer.parseInt( args[ 0 ] ) : 5;
        short[] sdata = new short[ nrow ];
        int[] idata = new int[ nrow ];
        long[] ldata = new long[ nrow ];
        float[] fdata = new float[ nrow ];
        double[] ddata = new double[ nrow ];
        String[] tdata = new String[ nrow ];
        String[] txts = { "zero", "one", null, "three", "", };
        for ( int ir = 0; ir < nrow; ir++ ) {
            sdata[ ir ] = (short) ir;
            idata[ ir ] = -ir;
            ldata[ ir ] = 4_000_000_000L + ir;
            fdata[ ir ] = ir == 2 ? Float.NaN : 0.25f + ir;
            ddata[ ir ] = ir == 2 ? Double.NaN : 0.5 + ir;
            tdata[ ir ] = txts[ ir % txts.length ];
        }
        FeatherColumnWriter[] writers = {
            PrimitiveArrayWriter.createShortWriter( "scol", sdata, null ),
            PrimitiveArrayWriter.createIntWriter( "icol", idata, "int col" ),
            PrimitiveArrayWriter.createLongWriter( "lcol", ldata, null ),
            PrimitiveArrayWriter.createFloatWriter( "fcol", fdata, null ),
            PrimitiveArrayWriter.createDoubleWriter( "dcol", ddata, null ),
            VariableLengthWriter.createStringWriter( "tcol", tdata, null, false)
        };
        new FeatherTableWriter( "test table", null, writers )
           .write( System.out );
    }
}
