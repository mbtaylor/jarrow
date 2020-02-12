
PYTHON_DIR = /mbt/local/pkg/miniconda3/bin
PYTHON = $(PYTHON_DIR)/python3

JARFILE = jarrow.jar

JSRC = \
       Dump.java \
       Feather.java \
       FeatherMeta.java \

build: $(JARFILE) data.fea

run: $(JARFILE) data.fea
	java -classpath $(JARFILE) Dump data.fea

jar: $(JARFILE)

data.fea: data.py
	$(PYTHON) data.py

$(JARFILE): $(JSRC) $(STIL_JAR)
	rm -rf tmp
	mkdir -p tmp
	javac -d tmp $(JSRC) \
            && jar cf $@ -C tmp .
	rm -rf tmp

clean:
	rm -f data.fea $(JARFILE)
	rm -rf tmp

