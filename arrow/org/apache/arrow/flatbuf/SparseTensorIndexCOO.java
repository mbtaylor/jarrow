// automatically generated by the FlatBuffers compiler, do not modify

package org.apache.arrow.flatbuf;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
/**
 * ----------------------------------------------------------------------
 * EXPERIMENTAL: Data structures for sparse tensors
 * Coordinate (COO) format of sparse tensor index.
 *
 * COO's index list are represented as a NxM matrix,
 * where N is the number of non-zero values,
 * and M is the number of dimensions of a sparse tensor.
 *
 * indicesBuffer stores the location and size of the data of this indices
 * matrix.  The value type and the stride of the indices matrix is
 * specified in indicesType and indicesStrides fields.
 *
 * For example, let X be a 2x3x4x5 tensor, and it has the following
 * 6 non-zero values:
 *
 *   X[0, 1, 2, 0] := 1
 *   X[1, 1, 2, 3] := 2
 *   X[0, 2, 1, 0] := 3
 *   X[0, 1, 3, 0] := 4
 *   X[0, 1, 2, 1] := 5
 *   X[1, 2, 0, 4] := 6
 *
 * In COO format, the index matrix of X is the following 4x6 matrix:
 *
 *   [[0, 0, 0, 0, 1, 1],
 *    [1, 1, 1, 2, 1, 2],
 *    [2, 2, 3, 1, 2, 0],
 *    [0, 1, 0, 0, 3, 4]]
 *
 * Note that the indices are sorted in lexicographical order.
 */
public final class SparseTensorIndexCOO extends Table {
  public static SparseTensorIndexCOO getRootAsSparseTensorIndexCOO(ByteBuffer _bb) { return getRootAsSparseTensorIndexCOO(_bb, new SparseTensorIndexCOO()); }
  public static SparseTensorIndexCOO getRootAsSparseTensorIndexCOO(ByteBuffer _bb, SparseTensorIndexCOO obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; vtable_start = bb_pos - bb.getInt(bb_pos); vtable_size = bb.getShort(vtable_start); }
  public SparseTensorIndexCOO __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  /**
   * The type of values in indicesBuffer
   */
  public Int indicesType() { return indicesType(new Int()); }
  public Int indicesType(Int obj) { int o = __offset(4); return o != 0 ? obj.__assign(__indirect(o + bb_pos), bb) : null; }
  /**
   * Non-negative byte offsets to advance one value cell along each dimension
   * If omitted, default to row-major order (C-like).
   */
  public long indicesStrides(int j) { int o = __offset(6); return o != 0 ? bb.getLong(__vector(o) + j * 8) : 0; }
  public int indicesStridesLength() { int o = __offset(6); return o != 0 ? __vector_len(o) : 0; }
  public ByteBuffer indicesStridesAsByteBuffer() { return __vector_as_bytebuffer(6, 8); }
  public ByteBuffer indicesStridesInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 6, 8); }
  /**
   * The location and size of the indices matrix's data
   */
  public Buffer indicesBuffer() { return indicesBuffer(new Buffer()); }
  public Buffer indicesBuffer(Buffer obj) { int o = __offset(8); return o != 0 ? obj.__assign(o + bb_pos, bb) : null; }

  public static void startSparseTensorIndexCOO(FlatBufferBuilder builder) { builder.startObject(3); }
  public static void addIndicesType(FlatBufferBuilder builder, int indicesTypeOffset) { builder.addOffset(0, indicesTypeOffset, 0); }
  public static void addIndicesStrides(FlatBufferBuilder builder, int indicesStridesOffset) { builder.addOffset(1, indicesStridesOffset, 0); }
  public static int createIndicesStridesVector(FlatBufferBuilder builder, long[] data) { builder.startVector(8, data.length, 8); for (int i = data.length - 1; i >= 0; i--) builder.addLong(data[i]); return builder.endVector(); }
  public static void startIndicesStridesVector(FlatBufferBuilder builder, int numElems) { builder.startVector(8, numElems, 8); }
  public static void addIndicesBuffer(FlatBufferBuilder builder, int indicesBufferOffset) { builder.addStruct(2, indicesBufferOffset, 0); }
  public static int endSparseTensorIndexCOO(FlatBufferBuilder builder) {
    int o = builder.endObject();
    builder.required(o, 4);  // indicesType
    builder.required(o, 8);  // indicesBuffer
    return o;
  }
}
