# Basic makefile for jarrow.
# Can autogenerate flatbuffer java source files if flatc is present.
# Otherwise, it just compiles the source files and builds javadocs.
# Some paths may need changing

JAVAC = /mbt/local/java/jdk1.6.0_41/bin/javac
JAVADOC = javadoc -Xdoclint:none

SRCDIR = java/uk/ac/bristol/star
FBSDIR = $(SRCDIR)/fbs

NAMESPACE = uk.ac.bristol.star.fbs.feather
PYTHON_DIR = /mbt/local/pkg/miniconda3/bin
PYTHON = $(PYTHON_DIR)/python3

FLATC = /mbt/github/flatbuffers/flatc

JARFILE = jarrow.jar

JSRC = \
       $(SRCDIR)/feather/AbstractColumnWriter.java \
       $(SRCDIR)/feather/BooleanRandomWriter.java \
       $(SRCDIR)/feather/Buf.java \
       $(SRCDIR)/feather/BufAccess.java \
       $(SRCDIR)/feather/BufMapper.java \
       $(SRCDIR)/feather/BufUtils.java \
       $(SRCDIR)/feather/ColStat.java \
       $(SRCDIR)/feather/Decoder.java \
       $(SRCDIR)/feather/FeatherColumn.java \
       $(SRCDIR)/feather/FeatherColumnWriter.java \
       $(SRCDIR)/feather/FeatherTable.java \
       $(SRCDIR)/feather/FeatherType.java \
       $(SRCDIR)/feather/FeatherWriter.java \
       $(SRCDIR)/feather/NumberRandomWriter.java \
       $(SRCDIR)/feather/Reader.java \
       $(SRCDIR)/feather/VariableLengthRandomWriter.java \

FBSRC = \
       $(FBSDIR)/google/ByteBufferUtil.java \
       $(FBSDIR)/google/Constants.java \
       $(FBSDIR)/google/FlatBufferBuilder.java \
       $(FBSDIR)/google/Struct.java \
       $(FBSDIR)/google/Table.java \
       $(FBSDIR)/google/Utf8.java \
       $(FBSDIR)/google/Utf8Safe.java \

FBFEATHERSRC = \
       $(FBSDIR)/feather/CategoryMetadata.java \
       $(FBSDIR)/feather/Column.java \
       $(FBSDIR)/feather/CTable.java \
       $(FBSDIR)/feather/DateMetadata.java \
       $(FBSDIR)/feather/Encoding.java \
       $(FBSDIR)/feather/PrimitiveArray.java \
       $(FBSDIR)/feather/TimeMetadata.java \
       $(FBSDIR)/feather/TimestampMetadata.java \
       $(FBSDIR)/feather/TimeUnit.java \
       $(FBSDIR)/feather/Type.java \
       $(FBSDIR)/feather/TypeMetadata.java \

build: $(JARFILE) javadocs data.fea

read: $(JARFILE) data.fea
	java -ea -classpath $(JARFILE) \
             uk.ac.bristol.star.feather.FeatherTable data.fea

write: test.fea

rw: test.fea
	java -ea -classpath $(JARFILE) \
             uk.ac.bristol.star.feather.FeatherTable test.fea

test.fea: $(JARFILE)
	java -ea -classpath $(JARFILE) \
             uk.ac.bristol.star.feather.FeatherWriter >$@

jar: $(JARFILE)

javadocs: $(JSRC) 
	rm -rf javadocs
	mkdir javadocs
	$(JAVADOC) -quiet \
                -classpath $(STIL_JAR):$(JSON_JAR) \
                -d javadocs \
                $(JSRC) $(FBSRC) $(FBFEATHERSRC)

data.fea: data.py
	$(PYTHON) data.py

big.fea: big.py
	$(PYTHON) big.py

feather_metadata.fbs: feather.fbs
	sed -e 's/^namespace.*/namespace $(NAMESPACE);/' \
            <feather.fbs >$@

$(FBSRC) $(FBFEATHERSRC):
	rm -rf $(FBSDIR)
	mkdir -p $(FBSDIR)
	cd java; \
        $(FLATC) --java ../feather_metadata.fbs

$(JARFILE): $(JSRC) $(FBSRC) $(FBFEATHERSRC)
	rm -rf tmp
	mkdir -p tmp
	$(JAVAC) -Xlint:all,-serial,-path -d tmp \
              $(JSRC) $(FBSRC) $(FBFEATHERSRC) \
            && jar cf $@ -C tmp .
	rm -rf tmp

clean:
	rm -f $(JARFILE)
	rm -f test.fea x.fea x.vot big.fea
	rm -rf tmp javadocs

veryclean: clean
	rm -f data.fea feather_metadata.fbs
	rm -rf $(FBSDIR)


