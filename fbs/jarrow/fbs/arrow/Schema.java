// automatically generated by the FlatBuffers compiler, do not modify

package jarrow.fbs.arrow;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
/**
 * ----------------------------------------------------------------------
 * A Schema describes the columns in a row batch
 */
public final class Schema extends Table {
  public static Schema getRootAsSchema(ByteBuffer _bb) { return getRootAsSchema(_bb, new Schema()); }
  public static Schema getRootAsSchema(ByteBuffer _bb, Schema obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; vtable_start = bb_pos - bb.getInt(bb_pos); vtable_size = bb.getShort(vtable_start); }
  public Schema __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  /**
   * endianness of the buffer
   * it is Little Endian by default
   * if endianness doesn't match the underlying system then the vectors need to be converted
   */
  public short endianness() { int o = __offset(4); return o != 0 ? bb.getShort(o + bb_pos) : 0; }
  public Field fields(int j) { return fields(new Field(), j); }
  public Field fields(Field obj, int j) { int o = __offset(6); return o != 0 ? obj.__assign(__indirect(__vector(o) + j * 4), bb) : null; }
  public int fieldsLength() { int o = __offset(6); return o != 0 ? __vector_len(o) : 0; }
  public KeyValue customMetadata(int j) { return customMetadata(new KeyValue(), j); }
  public KeyValue customMetadata(KeyValue obj, int j) { int o = __offset(8); return o != 0 ? obj.__assign(__indirect(__vector(o) + j * 4), bb) : null; }
  public int customMetadataLength() { int o = __offset(8); return o != 0 ? __vector_len(o) : 0; }

  public static int createSchema(FlatBufferBuilder builder,
      short endianness,
      int fieldsOffset,
      int custom_metadataOffset) {
    builder.startObject(3);
    Schema.addCustomMetadata(builder, custom_metadataOffset);
    Schema.addFields(builder, fieldsOffset);
    Schema.addEndianness(builder, endianness);
    return Schema.endSchema(builder);
  }

  public static void startSchema(FlatBufferBuilder builder) { builder.startObject(3); }
  public static void addEndianness(FlatBufferBuilder builder, short endianness) { builder.addShort(0, endianness, 0); }
  public static void addFields(FlatBufferBuilder builder, int fieldsOffset) { builder.addOffset(1, fieldsOffset, 0); }
  public static int createFieldsVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startFieldsVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static void addCustomMetadata(FlatBufferBuilder builder, int customMetadataOffset) { builder.addOffset(2, customMetadataOffset, 0); }
  public static int createCustomMetadataVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startCustomMetadataVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static int endSchema(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
  public static void finishSchemaBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }
  public static void finishSizePrefixedSchemaBuffer(FlatBufferBuilder builder, int offset) { builder.finishSizePrefixed(offset); }
}
