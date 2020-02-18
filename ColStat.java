package jarrow.feather;

public interface ColStat {
    long getRowCount();
    long getByteCount();
    long getNullCount();

    // Offset of data block (excluding mask bits) from start of written bytes
    long getDataOffset();
}
