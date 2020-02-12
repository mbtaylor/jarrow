
import java.io.File;
import java.io.IOException;

public class Dump {

    public static void main( String[] args ) throws IOException {
        Feather.fromFile( new File( args[ 0 ] ) );
        System.out.println( "OK" );
    }
}
