.PHONY: all clean
all: giraph-site.xml
clean:
	-rm giraph-site.xml

giraph-site.xml: giraph-site.xsl giraph-site.template.xml
	xsltproc --stringparam HOME $(HOME) $^ > $@



