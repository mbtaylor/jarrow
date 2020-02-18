package jarrow.feather;

public interface ColStat {
    long getByteCount();

    // Offset of data block (excluding mask bits) from start of written bytes
    long getDataOffset();
    long getNullCount();
}
