DPI ?= 400
SOURCES = $(shell find -name '*.dot')
TARGETS = $(SOURCES:.dot=.png)

.PHONY: all clean

%.png: %.dot
	dot -Tpng -Gdpi=${DPI} -o$@ $<

all: ${TARGETS}

clean:
	rm -f *.png

