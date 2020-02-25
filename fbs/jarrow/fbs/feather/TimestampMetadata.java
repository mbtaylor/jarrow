// automatically generated by the FlatBuffers compiler, do not modify

package jarrow.fbs.feather;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class TimestampMetadata extends Table {
  public static TimestampMetadata getRootAsTimestampMetadata(ByteBuffer _bb) { return getRootAsTimestampMetadata(_bb, new TimestampMetadata()); }
  public static TimestampMetadata getRootAsTimestampMetadata(ByteBuffer _bb, TimestampMetadata obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; vtable_start = bb_pos - bb.getInt(bb_pos); vtable_size = bb.getShort(vtable_start); }
  public TimestampMetadata __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public byte unit() { int o = __offset(4); return o != 0 ? bb.get(o + bb_pos) : 0; }
  /**
   * Timestamp data is assumed to be UTC, but the time zone is stored here for
   * presentation as localized
   */
  public String timezone() { int o = __offset(6); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer timezoneAsByteBuffer() { return __vector_as_bytebuffer(6, 1); }
  public ByteBuffer timezoneInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 6, 1); }

  public static int createTimestampMetadata(FlatBufferBuilder builder,
      byte unit,
      int timezoneOffset) {
    builder.startObject(2);
    TimestampMetadata.addTimezone(builder, timezoneOffset);
    TimestampMetadata.addUnit(builder, unit);
    return TimestampMetadata.endTimestampMetadata(builder);
  }

  public static void startTimestampMetadata(FlatBufferBuilder builder) { builder.startObject(2); }
  public static void addUnit(FlatBufferBuilder builder, byte unit) { builder.addByte(0, unit, 0); }
  public static void addTimezone(FlatBufferBuilder builder, int timezoneOffset) { builder.addOffset(1, timezoneOffset, 0); }
  public static int endTimestampMetadata(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
}
