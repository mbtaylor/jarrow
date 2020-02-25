
PYTHON_DIR = /mbt/local/pkg/miniconda3/bin
PYTHON = $(PYTHON_DIR)/python3
NAMESPACE = jarrow
JAVADOC_FLAGS = -Xdoclint:none

STIL_JAR = stil.jar
JSON_JAR = json.jar
STILTS_JAR = stilts.jar
STILTS = java -ea -classpath $(JARFILE):$(STILTS_JAR) \
         uk.ac.starlink.ttools.Stilts

FLATC = /mbt/github/flatbuffers/flatc

JARFILE = jarrow.jar

JSRC = \
       AbstractColumnWriter.java \
       BooleanColumnWriter.java \
       BufMapper.java \
       BufUtils.java \
       ColStat.java \
       Decoder.java \
       FeatherColumn.java \
       FeatherColumnWriter.java \
       FeatherTable.java \
       FeatherTableWriter.java \
       PrimitiveArrayWriter.java \
       Reader.java \
       VariableLengthWriter.java \

STIL_JSRC = \
       Buf.java \
       BufferAccess.java \
       \
       FeatherStarTable.java \
       FeatherStarTableWriter.java \
       FeatherTableBuilder.java \
       \
       AbstractItemAccumulator.java \
       BooleanStarColumnWriter.java \
       ItemAccumulator.java \
       NumberStarColumnWriter.java \
       StarColumnWriter.java \
       StarColumnWriters.java \
       VariableStarColumnWriter.java \

FBSRC = \
       fbs/com/google/flatbuffers/ByteBufferUtil.java \
       fbs/com/google/flatbuffers/Constants.java \
       fbs/com/google/flatbuffers/FlatBufferBuilder.java \
       fbs/com/google/flatbuffers/Struct.java \
       fbs/com/google/flatbuffers/Table.java \
       fbs/com/google/flatbuffers/Utf8.java \
       fbs/com/google/flatbuffers/Utf8Old.java \
       fbs/com/google/flatbuffers/Utf8Safe.java \
       fbs/jarrow/fbs/feather/CategoryMetadata.java \
       fbs/jarrow/fbs/feather/Column.java \
       fbs/jarrow/fbs/feather/CTable.java \
       fbs/jarrow/fbs/feather/DateMetadata.java \
       fbs/jarrow/fbs/feather/Encoding.java \
       fbs/jarrow/fbs/feather/PrimitiveArray.java \
       fbs/jarrow/fbs/feather/TimeMetadata.java \
       fbs/jarrow/fbs/feather/TimestampMetadata.java \
       fbs/jarrow/fbs/feather/TimeUnit.java \
       fbs/jarrow/fbs/feather/Type.java \
       fbs/jarrow/fbs/feather/TypeMetadata.java \
       \
       fbs/jarrow/fbs/arrow/Binary.java \
       fbs/jarrow/fbs/arrow/Block.java \
       fbs/jarrow/fbs/arrow/Bool.java \
       fbs/jarrow/fbs/arrow/Buffer.java \
       fbs/jarrow/fbs/arrow/Date.java \
       fbs/jarrow/fbs/arrow/DateUnit.java \
       fbs/jarrow/fbs/arrow/Decimal.java \
       fbs/jarrow/fbs/arrow/DictionaryBatch.java \
       fbs/jarrow/fbs/arrow/DictionaryEncoding.java \
       fbs/jarrow/fbs/arrow/DictionaryKind.java \
       fbs/jarrow/fbs/arrow/Duration.java \
       fbs/jarrow/fbs/arrow/Endianness.java \
       fbs/jarrow/fbs/arrow/Field.java \
       fbs/jarrow/fbs/arrow/FieldNode.java \
       fbs/jarrow/fbs/arrow/FixedSizeBinary.java \
       fbs/jarrow/fbs/arrow/FixedSizeList.java \
       fbs/jarrow/fbs/arrow/FloatingPoint.java \
       fbs/jarrow/fbs/arrow/Footer.java \
       fbs/jarrow/fbs/arrow/Interval.java \
       fbs/jarrow/fbs/arrow/IntervalUnit.java \
       fbs/jarrow/fbs/arrow/Int.java \
       fbs/jarrow/fbs/arrow/KeyValue.java \
       fbs/jarrow/fbs/arrow/LargeBinary.java \
       fbs/jarrow/fbs/arrow/LargeList.java \
       fbs/jarrow/fbs/arrow/LargeUtf8.java \
       fbs/jarrow/fbs/arrow/List.java \
       fbs/jarrow/fbs/arrow/Map.java \
       fbs/jarrow/fbs/arrow/MessageHeader.java \
       fbs/jarrow/fbs/arrow/Message.java \
       fbs/jarrow/fbs/arrow/MetadataVersion.java \
       fbs/jarrow/fbs/arrow/Null.java \
       fbs/jarrow/fbs/arrow/Precision.java \
       fbs/jarrow/fbs/arrow/RecordBatch.java \
       fbs/jarrow/fbs/arrow/Schema.java \
       fbs/jarrow/fbs/arrow/SparseMatrixCompressedAxis.java \
       fbs/jarrow/fbs/arrow/SparseMatrixIndexCSX.java \
       fbs/jarrow/fbs/arrow/SparseTensorIndexCOO.java \
       fbs/jarrow/fbs/arrow/SparseTensorIndex.java \
       fbs/jarrow/fbs/arrow/SparseTensor.java \
       fbs/jarrow/fbs/arrow/Struct_.java \
       fbs/jarrow/fbs/arrow/TensorDim.java \
       fbs/jarrow/fbs/arrow/Tensor.java \
       fbs/jarrow/fbs/arrow/Time.java \
       fbs/jarrow/fbs/arrow/Timestamp.java \
       fbs/jarrow/fbs/arrow/TimeUnit.java \
       fbs/jarrow/fbs/arrow/Type.java \
       fbs/jarrow/fbs/arrow/Union.java \
       fbs/jarrow/fbs/arrow/UnionMode.java \
       fbs/jarrow/fbs/arrow/Utf8.java \

build: $(JARFILE) javadocs data.fea

read: $(JARFILE) data.fea
	java -ea -classpath $(JARFILE) jarrow.feather.FeatherTable data.fea

write: test.fea

rw: test.fea
	java -ea -classpath $(JARFILE) jarrow.feather.FeatherTable test.fea

rws: data.fea $(JARFILE) $(JSON_JAR) $(STILTS_JAR)
	$(STILTS) tpipe \
               in=data.fea ifmt=uk.ac.starlink.feather.FeatherTableBuilder \
               cmd='colmeta -units km/s 2' \
               cmd='colmeta -ucd meta.code 3' \
               out=x.vot && \
	$(STILTS) tpipe in=x.vot && \
        $(STILTS) tpipe \
               in=x.vot \
               ofmt=uk.ac.starlink.feather.FeatherStarTableWriter out=x.fea && \
        $(STILTS) tpipe \
               in=x.fea ifmt=uk.ac.starlink.feather.FeatherTableBuilder \
               cmd=meta && \
        $(STILTS) tpipe \
               in=x.fea ifmt=uk.ac.starlink.feather.FeatherTableBuilder \

test.fea: $(JARFILE)
	java -ea -classpath $(JARFILE) jarrow.feather.FeatherTableWriter >$@

jar: $(JARFILE)

javadocs: $(JSRC) $(STIL_JSRC)
	rm -rf javadocs
	mkdir javadocs
	javadoc $(JAVADOC_FLAGS) -quiet \
                -classpath $(STIL_JAR):$(JSON_JAR) \
                -d javadocs \
                $(JSRC) $(STIL_JSRC) $(FBSRC)

data.fea: data.py
	$(PYTHON) data.py

big.fea: big.py
	$(PYTHON) big.py

tostarjava: $(JARFILE)
	for d in /mbt/starjava/source/ttools/src/lib/ \
                 /mbt/starjava/source/ttools/lib/ttools/ \
                 /mbt/starjava/lib/ttools/; \
        do cp $(JARFILE) $$d; done

$(STIL_JAR):
	curl -OL http://www.starlink.ac.uk/stil/stil.jar

$(STILTS_JAR):
	cp /mbt/starjava/lib/ttools/stilts.jar .

$(JSON_JAR):
	cp /mbt/starjava/lib/ttools/json.jar .

$(NAMESPACE)_metadata.fbs: feather.fbs
	sed -e 's/^namespace.*/namespace $(NAMESPACE).fbs;/' \
            <feather.fbs >$@

fbs/$(NAMESPACE): $(NAMESPACE)_metadata.fbs
	rm -rf fbs/$(NAMESPACE)
	mkdir -p fbs/$(NAMESPACE)
	cd fbs; \
        $(FLATC) --java ../$(NAMESPACE)_metadata.fbs

$(JARFILE): $(JSRC) $(STIL_JSRC) $(FBSRC) $(STIL_JAR) $(JSON_JAR)
	rm -rf tmp
	mkdir -p tmp
	javac -Xlint:all,-serial,-path -d tmp \
              -classpath $(STIL_JAR):$(JSON_JAR) \
              $(JSRC) $(STIL_JSRC) $(FBSRC) \
            && jar cf $@ -C tmp .
	rm -rf tmp

clean:
	rm -f $(JARFILE) $(NAMESPACE)_metadata.fbs $(JSON_JAR) $(STIL_JAR) \
              test.fea x.fea x.vot big.fea
	rm -rf tmp javadocs

veryclean: clean
	rm -f $(NAMESPACE)_metadata.fbs data.fea $(STILTS_JAR)
	rm -rf fbs/jarrow


