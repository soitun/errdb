
all: deps
	./rebar compile

deps:
	./rebar get-deps

dist:
	rm -rf rel/errdb
	./rebar generate

clean:
	./rebar clean

