
PYTHON_DIR = /mbt/local/pkg/miniconda3/bin
PYTHON = $(PYTHON_DIR)/python3
NAMESPACE = jarrow
JAVADOC_FLAGS = -Xdoclint:none

FLATC = /mbt/github/flatbuffers/flatc

JARFILE = jarrow.jar

JSRC = \
       AbstractColumnWriter.java \
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

FBSRC = \
       fbs/com/google/flatbuffers/ByteBufferUtil.java \
       fbs/com/google/flatbuffers/Constants.java \
       fbs/com/google/flatbuffers/FlatBufferBuilder.java \
       fbs/com/google/flatbuffers/Struct.java \
       fbs/com/google/flatbuffers/Table.java \
       fbs/com/google/flatbuffers/Utf8.java \
       fbs/com/google/flatbuffers/Utf8Old.java \
       fbs/com/google/flatbuffers/Utf8Safe.java \
       fbs/jarrow/fbs/CategoryMetadata.java \
       fbs/jarrow/fbs/Column.java \
       fbs/jarrow/fbs/CTable.java \
       fbs/jarrow/fbs/DateMetadata.java \
       fbs/jarrow/fbs/Encoding.java \
       fbs/jarrow/fbs/PrimitiveArray.java \
       fbs/jarrow/fbs/TimeMetadata.java \
       fbs/jarrow/fbs/TimestampMetadata.java \
       fbs/jarrow/fbs/TimeUnit.java \
       fbs/jarrow/fbs/Type.java \
       fbs/jarrow/fbs/TypeMetadata.java \


build: $(JARFILE) javadocs data.fea

read: $(JARFILE) data.fea
	java -ea -classpath $(JARFILE) jarrow.feather.FeatherTable data.fea

write: test.fea

rw: test.fea
	java -ea -classpath $(JARFILE) jarrow.feather.FeatherTable test.fea

test.fea: $(JARFILE)
	java -ea -classpath $(JARFILE) jarrow.feather.FeatherTableWriter >$@

jar: $(JARFILE)

javadocs: $(JSRC)
	rm -rf javadocs
	mkdir javadocs
	javadoc $(JAVADOC_FLAGS) -quiet \
                -d javadocs $(JSRC) $(FBSRC)

data.fea: data.py
	$(PYTHON) data.py

$(NAMESPACE)_metadata.fbs: feather_metadata.fbs
	sed -e 's/^namespace.*/namespace $(NAMESPACE).fbs;/' \
            <feather_metadata.fbs >$@

fbs/$(NAMESPACE): $(NAMESPACE)_metadata.fbs
	rm -rf fbs/$(NAMESPACE)
	mkdir -p fbs/$(NAMESPACE)
	cd fbs; \
        $(FLATC) --java ../$(NAMESPACE)_metadata.fbs

$(JARFILE): $(JSRC) $(FBSRC) $(STIL_JAR)
	rm -rf tmp
	mkdir -p tmp
	javac -Xlint:all,-serial,-path -d tmp $(JSRC) $(FBSRC) \
            && jar cf $@ -C tmp .
	rm -rf tmp

clean:
	rm -f $(JARFILE) $(NAMESPACE)_metadata.fbs test.fea
	rm -rf tmp javadocs

veryclean: clean
	rm -f $(NAMESPACE)_metadata.fbs data.fea
	rm -rf fbs/jarrow


