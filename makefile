
PYTHON_DIR = /mbt/local/pkg/miniconda3/bin
PYTHON = $(PYTHON_DIR)/python3
NAMESPACE = jarrow

FLATC = /mbt/github/flatbuffers/flatc

JARFILE = jarrow.jar

JSRC = \
       Dump.java \
       Feather.java \
       FeatherMeta.java \

FBSRC = `find fbs -name "*.java" -print`

build: $(JARFILE) data.fea

run: $(JARFILE) data.fea
	java -classpath $(JARFILE) Dump data.fea

jar: $(JARFILE)

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

$(JARFILE): $(JSRC) $(STIL_JAR)
	rm -rf tmp
	mkdir -p tmp
	javac -d tmp $(JSRC) $(FBSRC) \
            && jar cf $@ -C tmp .
	rm -rf tmp

clean:
	rm -f data.fea $(JARFILE)
	rm -rf tmp

veryclean: clean
	rm -f $(NAMESPACE)_metadata.fbs
	rm -rf fbs/jarrow


