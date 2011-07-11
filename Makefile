PLATFORM=/opt/platform
all: deps
	(cd src;$(MAKE))

deps:
	cp ${PLATFORM}/elog/ebin/* ebin
	cp ${PLATFORM}/elog/include/* include 
	cp ${PLATFORM}/core/ebin/* ebin
	cp ${PLATFORM}/mochiweb/ebin/* ebin

clean:
	(cd src;$(MAKE) clean)

