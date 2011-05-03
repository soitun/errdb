# Redis Makefile
# Copyright (C) 2009 Salvatore Sanfilippo <antirez at gmail dot com>
# This file is released under the BSD license, see the COPYING file

release_hdr := $(shell sh -c './mkreleasehdr.sh')
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')
OPTIMIZATION?=-O2
ifeq ($(uname_S),SunOS)
  CFLAGS?= -std=c99 -pedantic $(OPTIMIZATION) -Wall -W -D__EXTENSIONS__ -D_XPG6
  CCLINK?= -ldl -lnsl -lsocket -lm -lpthread
  DEBUG?= -g -ggdb 
else
  CFLAGS?= -std=c99 -pedantic $(OPTIMIZATION) -Wall -W $(ARCH) $(PROF)
  CCLINK?= -lm -pthread
  DEBUG?= -g -rdynamic -ggdb 
endif

ifeq ($(USE_TCMALLOC),yes)
  CCLINK+= -ltcmalloc
  CFLAGS+= -DUSE_TCMALLOC
endif
CCOPT= $(CFLAGS) $(CCLINK) $(ARCH) $(PROF)

PREFIX= /usr/local
INSTALL_BIN= $(PREFIX)/bin
INSTALL= cp -p

OBJ = debug.o adlist.o ae.o anet.o dict.o errdb.o sds.o zmalloc.o pqsort.o sha1.o release.o client.o util.o db.o t_string.o ts_list.o  config.o syncio.o endian.o 
BENCHOBJ = ae.o anet.o sds.o adlist.o zmalloc.o
CLIOBJ = anet.o sds.o adlist.o errdb-cli.o zmalloc.o release.o

PRGNAME = errdb
CLIPRGNAME = errdb-cli

all: errdb-cli errdb
	@echo ""
	@echo "Success!"
	@echo ""

# Deps (use make dep to generate this)
adlist.o: adlist.c adlist.h zmalloc.h
ae.o: ae.c ae.h zmalloc.h config.h ae_kqueue.c
ae_epoll.o: ae_epoll.c
ae_kqueue.o: ae_kqueue.c
ae_select.o: ae_select.c
anet.o: anet.c fmacros.h anet.h
config.o: config.c errdb.h fmacros.h config.h ae.h sds.h dict.h adlist.h \
  zmalloc.h anet.h  version.h
db.o: db.c errdb.h fmacros.h config.h ae.h sds.h dict.h adlist.h \
  zmalloc.h anet.h  version.h
dict.o: dict.c fmacros.h dict.h zmalloc.h
client.o: client.c errdb.h fmacros.h config.h ae.h sds.h dict.h \
  adlist.h zmalloc.h anet.h  version.h
pqsort.o: pqsort.c
errdb-cli.o: errdb-cli.c fmacros.h version.h ../deps/hierrdb/hierrdb.h \
  sds.h zmalloc.h ../deps/linenoise/linenoise.h help.h
errdb.o: errdb.c errdb.h fmacros.h config.h ae.h sds.h dict.h adlist.h \
  zmalloc.h anet.h version.h
release.o: release.c release.h
sds.o: sds.c sds.h zmalloc.h
sha1.o: sha1.c sha1.h
syncio.o: syncio.c errdb.h fmacros.h config.h ae.h sds.h dict.h adlist.h \
  zmalloc.h anet.h  version.h
ts_list.o: ts_list.c errdb.h fmacros.h config.h ae.h sds.h dict.h adlist.h \
  zmalloc.h anet.h  version.h
t_string.o: t_string.c errdb.h fmacros.h config.h ae.h sds.h dict.h \
  adlist.h zmalloc.h anet.h  version.h
util.o: util.c errdb.h fmacros.h config.h ae.h sds.h dict.h adlist.h \
  zmalloc.h anet.h version.h
debug.o: debug.c errdb.h fmacros.h config.h ae.h sds.h dict.h adlist.h \
  zmalloc.h anet.h  version.h
zmalloc.o: zmalloc.c config.h

dependencies:
	cd ../deps/hierrdb && $(MAKE) static ARCH="$(ARCH)"
	cd ../deps/linenoise && $(MAKE) ARCH="$(ARCH)"

errdb: $(OBJ)
	$(CC) -o $(PRGNAME) $(CCOPT) $(DEBUG) $(OBJ)

errdb-cli: dependencies $(CLIOBJ)
	$(CC) -o $(CLIPRGNAME) $(CCOPT) $(DEBUG) $(CLIOBJ) ../deps/hierrdb/libhierrdb.a ../deps/linenoise/linenoise.o

errdb-cli.o:
	$(CC) -c $(CFLAGS) -I../deps/hierrdb -I../deps/linenoise $(DEBUG) $(COMPILE_TIME) $<

.c.o:
	$(CC) -c $(CFLAGS) $(DEBUG) $(COMPILE_TIME) $<

clean:
	rm -rf $(PRGNAME) $(BENCHPRGNAME) $(CLIPRGNAME) $(CHECKDUMPPRGNAME) $(CHECKAOFPRGNAME) *.o *.gcda *.gcno *.gcov

dep:
	$(CC) -MM *.c -I ../deps/hierrdb -I ../deps/linenoise

test: errdb
	(cd ..; tclsh8.5 tests/test_helper.tcl --tags "${TAGS}" --file "${FILE}")

log:
	git log '--pretty=format:%ad %s (%cn)' --date=short > ../Changelog

32bit:
	@echo ""
	@echo "WARNING: if it fails under Linux you probably need to install libc6-dev-i386"
	@echo ""
	$(MAKE) ARCH="-m32"

gprof:
	$(MAKE) PROF="-pg"

gcov:
	$(MAKE) PROF="-fprofile-arcs -ftest-coverage"

noopt:
	$(MAKE) OPTIMIZATION=""

32bitgprof:
	$(MAKE) PROF="-pg" ARCH="-arch i386"

install: all
	mkdir -p $(INSTALL_BIN)
	$(INSTALL) $(PRGNAME) $(INSTALL_BIN)
	$(INSTALL) $(BENCHPRGNAME) $(INSTALL_BIN)
	$(INSTALL) $(CLIPRGNAME) $(INSTALL_BIN)
	$(INSTALL) $(CHECKDUMPPRGNAME) $(INSTALL_BIN)
	$(INSTALL) $(CHECKAOFPRGNAME) $(INSTALL_BIN)
