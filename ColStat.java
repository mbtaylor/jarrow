package jarrow.feather;

public interface ColStat {
    long getRowCount();

    // Total number of bytes written.
    long getByteCount();
    long getNullCount();

    // Offset of data block from start of written bytes.
    // This offset skips the mask bits, but not the variable length array.
    long getDataOffset();
}
