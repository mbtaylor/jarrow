package jarrow.feather;

import jarrow.fbs.Type;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;
import java.util.logging.Logger;

@SuppressWarnings("cast")
public abstract class Decoder<T> {

    private final Class<T> clazz_;
    private static final Logger logger_ =
        Logger.getLogger( Decoder.class.getName() );

    private Decoder( Class<T> clazz ) {
        clazz_ = clazz;
    }

    public abstract Reader<T> createReader( ByteBuffer buf );

    public Class<T> getValueClass() {
        return clazz_;
    }

    private static final Decoder<?>[] TYPE_DECODERS = createTypeDecoders();
    private static final Decoder<?> UNSUPPORTED = new UnsupportedDecoder();

    public static Decoder<?> createDecoder( byte type, boolean hasNulls ) {
        Decoder<?> decoder = type >= 0 && type <= TYPE_DECODERS.length
                           ? TYPE_DECODERS[ type ]
                           : null;
        if ( decoder != null ) {
 //         return hasNulls ? new MaskDecoder( decoder ) : decoder;
 return decoder;
        }
        else {
            logger_.warning( "No decoder for data type " + type );
            return UNSUPPORTED;
        }
    }

    private static Decoder<?>[] createTypeDecoders() {
        Decoder<?>[] decoders = new Decoder<?>[ 20 ];
        decoders[ Type.BOOL ] = new Decoder<Boolean>( Boolean.class ) {
            public Reader<Boolean> createReader( final ByteBuffer bbuf ) {
                return new AbstractReader<Boolean>( Boolean.class ) {
                    private boolean get( long ix ) {
                        return ( bbuf.get( longToInt( ix / 8 ) )
                               & ( 1 << ((int) ix) % 8 ) ) != 0;
                    }
                    public Boolean getObject( long ix ) {
                        return Boolean.valueOf( get( ix ) );
                    }
                    public byte getByte( long ix ) {
                        return get( ix ) ? (byte) 1 : (byte) 0;
                    }
                    public short getShort( long ix ) {
                        return get( ix ) ? (short) 1 : (short) 0;
                    }
                    public int getInt( long ix ) {
                        return get( ix ) ? 1 : 0;
                    }
                    public long getLong( long ix ) {
                        return get( ix ) ? 1L : 0L;
                    }
                    public float getFloat( long ix ) {
                        return get( ix ) ? 1f : 0f;
                    }
                    public double getDouble( long ix ) {
                        return get( ix ) ? 1. : 0.;
                    }
                };
            };
        };
        decoders[ Type.INT8 ] = new Decoder<Byte>( Byte.class ) {
            public Reader<Byte> createReader( final ByteBuffer bbuf ) {
                return new AbstractReader<Byte>( Byte.class ) {
                    private byte get( long ix ) {
                        return bbuf.get( longToInt( ix ) );
                    }
                    public Byte getObject( long ix ) {
                        return Byte.valueOf( get( ix ) );
                    }
                    public byte getByte( long ix ) {
                        return (byte) get( ix );
                    }
                    public short getShort( long ix ) {
                        return (short) get( ix );
                    }
                    public int getInt( long ix ) {
                        return (int) get( ix );
                    }
                    public long getLong( long ix ) {
                        return (long) get( ix );
                    }
                    public float getFloat( long ix ) {
                        return (float) get( ix );
                    }
                    public double getDouble( long ix ) {
                        return (double) get( ix );
                    }
                };
            }
        };
        decoders[ Type.INT16 ] = new Decoder<Short>( Short.class ) {
            public Reader<Short> createReader( ByteBuffer bbuf ) {
                final ShortBuffer sbuf = bbuf.asShortBuffer();
                return new ShortReader() {
                    short get( long ix ) {
                        return sbuf.get( longToInt( ix ) );
                    }
                };
            }
        };
        decoders[ Type.INT32 ] = new Decoder<Integer>( Integer.class ) {
            public Reader<Integer> createReader( ByteBuffer bbuf ) {
                final IntBuffer ibuf = bbuf.asIntBuffer();
                return new IntReader() {
                    int get( long ix ) {
                        return ibuf.get( longToInt( ix ) );
                    }
                };
            }
        };
        decoders[ Type.INT64 ] = new Decoder<Long>( Long.class ) {
            public Reader<Long> createReader( ByteBuffer bbuf ) {
                final LongBuffer lbuf = bbuf.asLongBuffer();
                return new LongReader() {
                    long get( long ix ) {
                        return lbuf.get( longToInt( ix ) );
                    }
                };
            }
        };
        decoders[ Type.UINT8 ] = new Decoder<Short>( Short.class ) {
            public Reader<Short> createReader( final ByteBuffer bbuf ) {
                return new ShortReader() {
                    short get( long ix ) {
                        return (short) ( 0xff & bbuf.get( longToInt( ix ) ) );
                    }
                };
            }
        };
        decoders[ Type.UINT16 ] = new Decoder<Integer>( Integer.class ) {
            public Reader<Integer> createReader( ByteBuffer bbuf ) {
                final ShortBuffer sbuf = bbuf.asShortBuffer();
                return new IntReader() {
                    int get( long ix ) {
                        return 0xffff & sbuf.get( longToInt( ix ) );
                    }
                };
            }
        };
        decoders[ Type.UINT32 ] = new Decoder<Long>( Long.class ) {
            public Reader<Long> createReader( ByteBuffer bbuf ) {
                final IntBuffer ibuf = bbuf.asIntBuffer();
                return new LongReader() {
                    long get( long ix ) {
                        return 0xffffffffL & ibuf.get( longToInt( ix ) );
                    }
                };
            }
        };
        decoders[ Type.UINT64 ] = UNSUPPORTED;
        decoders[ Type.FLOAT ] = new Decoder<Float>( Float.class ) {
            public Reader<Float> createReader( ByteBuffer bbuf ) {
                final FloatBuffer fbuf = bbuf.asFloatBuffer();
                return new AbstractReader<Float>( Float.class ) {
                    private float get( long ix ) {
                        return fbuf.get( longToInt( ix ) );
                    }
                    public Float getObject( long ix ) {
                        return Float.valueOf( get( ix ) );
                    }
                    public byte getByte( long ix ) {
                        return (byte) get( ix );
                    }
                    public short getShort( long ix ) {
                        return (short) get( ix );
                    }
                    public int getInt( long ix ) {
                        return (int) get( ix );
                    }
                    public long getLong( long ix ) {
                        return (long) get( ix );
                    }
                    public float getFloat( long ix ) {
                        return (float) get( ix );
                    }
                    public double getDouble( long ix ) {
                        return (double) get( ix );
                    }
                };
            }
        };
        decoders[ Type.DOUBLE ] = new Decoder<Double>( Double.class ) {
            public Reader<Double> createReader( ByteBuffer bbuf ) {
                final DoubleBuffer dbuf = bbuf.asDoubleBuffer();
                return new AbstractReader<Double>( Double.class ) {
                    private double get( long ix ) {
                        return dbuf.get( longToInt( ix ) );
                    }
                    public Double getObject( long ix ) {
                        return Double.valueOf( get( ix ) );
                    }
                    public byte getByte( long ix ) {
                        return (byte) get( ix );
                    }
                    public short getShort( long ix ) {
                        return (short) get( ix );
                    }
                    public int getInt( long ix ) {
                        return (int) get( ix );
                    }
                    public long getLong( long ix ) {
                        return (long) get( ix );
                    }
                    public float getFloat( long ix ) {
                        return (float) get( ix );
                    }
                    public double getDouble( long ix ) {
                        return (double) get( ix );
                    }
                };
            }
        };
        return decoders;
    }

    private static abstract class AbstractReader<T> implements Reader<T> {
        final Class<T> clazz_;
        AbstractReader( Class<T> clazz ) {
            clazz_ = clazz;
        }
        public Class<T> getValueClass() {
            return clazz_;
        }
        int longToInt( long index ) {
            int ix = (int) index;
            if ( ix == index ) {
                return ix;
            }
            else {
                throw new UnsupportedOperationException( "Buffer too long!" );
            }
        }
    }

    private static abstract class NonNumericReader<T>
            extends AbstractReader<T> {
        public NonNumericReader( Class<T> clazz ) {
            super( clazz );
        }
        public byte getByte( long ix ) {
            return (byte) 0;
        }
        public short getShort( long ix ) {
            return (short) 0;
        }
        public int getInt( long ix ) {
            return 0;
        }
        public long getLong( long ix ) {
            return 0L;
        }
        public float getFloat( long ix ) {
            return Float.NaN;
        }
        public double getDouble( long ix ) {
            return Double.NaN;
        }
    }

    private static abstract class ShortReader extends AbstractReader<Short> {
        ShortReader() {
            super( Short.class );
        }
        abstract short get( long ix );
        public Short getObject( long ix ) {
            return Short.valueOf( get( ix ) );
        }
        public byte getByte( long ix ) {
            return (byte) get( ix );
        }
        public short getShort( long ix ) {
            return (short) get( ix );
        }
        public int getInt( long ix ) {
            return (int) get( ix );
        }
        public long getLong( long ix ) {
            return (long) get( ix );
        }
        public float getFloat( long ix ) {
            return (float) get( ix );
        }
        public double getDouble( long ix ) {
            return (double) get( ix );
        }
    }

    private static abstract class IntReader extends AbstractReader<Integer> {
        IntReader() {
            super( Integer.class );
        }
        abstract int get( long ix );
        public Integer getObject( long ix ) {
            return Integer.valueOf( get( ix ) );
        }
        public byte getByte( long ix ) {
            return (byte) get( ix );
        }
        public short getShort( long ix ) {
            return (short) get( ix );
        }
        public int getInt( long ix ) {
            return (int) get( ix );
        }
        public long getLong( long ix ) {
            return (long) get( ix );
        }
        public float getFloat( long ix ) {
            return (float) get( ix );
        }
        public double getDouble( long ix ) {
            return (double) get( ix );
        }
    }

    private static abstract class LongReader extends AbstractReader<Long> {
        LongReader() {
            super( Long.class );
        }
        abstract long get( long ix );
        public Long getObject( long ix ) {
            return Long.valueOf( get( ix ) );
        }
        public byte getByte( long ix ) {
            return (byte) get( ix );
        }
        public short getShort( long ix ) {
            return (short) get( ix );
        }
        public int getInt( long ix ) {
            return (int) get( ix );
        }
        public long getLong( long ix ) {
            return (long) get( ix );
        }
        public float getFloat( long ix ) {
            return (float) get( ix );
        }
        public double getDouble( long ix ) {
            return (double) get( ix );
        }
    }

    private static class UnsupportedDecoder extends Decoder<Void> {
        private static Reader<Void> dummyReader_;
        UnsupportedDecoder() {
            super( Void.class );
            dummyReader_ = new NonNumericReader<Void>( Void.class ) {
                public Void getObject( long ix ) {
                    return null;
                }
            };
        }
        public Reader<Void> createReader( ByteBuffer buf ) {
            return dummyReader_;
        }
    }
}
