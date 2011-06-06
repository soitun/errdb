all: deps
	(cd src;$(MAKE))

deps:
	cp /opt/platform/elog/ebin/* ebin
	cp /opt/platform/elog/include/* include 
	cp /opt/platform/core/ebin/* ebin
	cp /opt/platform/mochiweb/ebin/* ebin

clean:
	(cd src;$(MAKE) clean)

