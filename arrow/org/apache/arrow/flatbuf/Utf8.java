// automatically generated by the FlatBuffers compiler, do not modify

package org.apache.arrow.flatbuf;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
/**
 * Unicode with UTF-8 encoding
 */
public final class Utf8 extends Table {
  public static Utf8 getRootAsUtf8(ByteBuffer _bb) { return getRootAsUtf8(_bb, new Utf8()); }
  public static Utf8 getRootAsUtf8(ByteBuffer _bb, Utf8 obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; vtable_start = bb_pos - bb.getInt(bb_pos); vtable_size = bb.getShort(vtable_start); }
  public Utf8 __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }


  public static void startUtf8(FlatBufferBuilder builder) { builder.startObject(0); }
  public static int endUtf8(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
}
