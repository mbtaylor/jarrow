package jarrow.feather;

public interface Reader {
    Object getObject( long ix );
    byte getByte( long ix );
    short getShort( long ix );
    int getInt( long ix );
    long getLong( long ix );
    float getFloat( long ix );
    double getDouble( long ix );
    void close();
}
