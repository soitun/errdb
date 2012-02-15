PLATFORM=/opt/platform

all: deps
	./rebar compile

deps:
	cp platform/elog/ebin/* ebin
	cp platform/elog/include/* include 
	cp ${PLATFORM}/core/ebin/* ebin
	cp ${PLATFORM}/mochiweb/ebin/* ebin

clean:
	./rebar clean

