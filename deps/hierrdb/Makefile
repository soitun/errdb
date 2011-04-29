# Hierrdb Makefile
# Copyright (C) 2010 Salvatore Sanfilippo <antirez at gmail dot com>
# This file is released under the BSD license, see the COPYING file

OBJ = net.o hierrdb.o sds.o async.o
BINS = hierrdb-example hierrdb-test

uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')
OPTIMIZATION?=-O3
ifeq ($(uname_S),SunOS)
  CFLAGS?=-std=c99 -pedantic $(OPTIMIZATION) -fPIC -Wall -W -D__EXTENSIONS__ -D_XPG6 $(ARCH) $(PROF)
  CCLINK?=-ldl -lnsl -lsocket -lm -lpthread
  LDFLAGS?=-L. -Wl,-R,.
  DYLIBNAME?=libhierrdb.so
  DYLIB_MAKE_CMD?=$(CC) -G -o ${DYLIBNAME} ${OBJ}
  STLIBNAME?=libhierrdb.a
  STLIB_MAKE_CMD?=ar rcs ${STLIBNAME} ${OBJ}
else ifeq ($(uname_S),Darwin)
  CFLAGS?=-std=c99 -pedantic $(OPTIMIZATION) -fPIC -Wall -W -Wwrite-strings $(ARCH) $(PROF)
  CCLINK?=-lm -pthread
  LDFLAGS?=-L. -Wl,-rpath,.
  OBJARCH?=-arch i386 -arch x86_64
  DYLIBNAME?=libhierrdb.dylib
  DYLIB_MAKE_CMD?=libtool -dynamic -o ${DYLIBNAME} -lm ${DEBUG} - ${OBJ}
  STLIBNAME?=libhierrdb.a
  STLIB_MAKE_CMD?=libtool -static -o ${STLIBNAME} - ${OBJ}
else
  CFLAGS?=-std=c99 -pedantic $(OPTIMIZATION) -fPIC -Wall -W -Wwrite-strings $(ARCH) $(PROF)
  CCLINK?=-lm -pthread
  LDFLAGS?=-L. -Wl,-rpath,.
  DYLIBNAME?=libhierrdb.so
  DYLIB_MAKE_CMD?=gcc -shared -Wl,-soname,${DYLIBNAME} -o ${DYLIBNAME} ${OBJ}
  STLIBNAME?=libhierrdb.a
  STLIB_MAKE_CMD?=ar rcs ${STLIBNAME} ${OBJ}
endif
CCOPT= $(CFLAGS) $(CCLINK)
DEBUG?= -g -ggdb

PREFIX?= /usr/local
INSTALL_INC= $(PREFIX)/include/hierrdb
INSTALL_LIB= $(PREFIX)/lib
INSTALL= cp -a

all: ${DYLIBNAME} ${BINS}

# Deps (use make dep to generate this)
net.o: net.c fmacros.h net.h
async.o: async.c async.h hierrdb.h sds.h util.h
example.o: example.c hierrdb.h
hierrdb.o: hierrdb.c hierrdb.h net.h sds.h util.h
sds.o: sds.c sds.h
test.o: test.c hierrdb.h

${DYLIBNAME}: ${OBJ}
	${DYLIB_MAKE_CMD}

${STLIBNAME}: ${OBJ}
	${STLIB_MAKE_CMD}

dynamic: ${DYLIBNAME}
static: ${STLIBNAME}

# Binaries:
hierrdb-example-libevent: example-libevent.c adapters/libevent.h ${DYLIBNAME}
	$(CC) -o $@ $(CCOPT) $(DEBUG) $(LDFLAGS) -lhierrdb -levent example-libevent.c

hierrdb-example-libev: example-libev.c adapters/libev.h ${DYLIBNAME}
	$(CC) -o $@ $(CCOPT) $(DEBUG) $(LDFLAGS) -lhierrdb -lev example-libev.c

ifndef AE_DIR
hierrdb-example-ae:
	@echo "Please specify AE_DIR (e.g. <errdb repository>/src)"
	@false
else
hierrdb-example-ae: example-ae.c adapters/ae.h ${DYLIBNAME}
	$(CC) -o $@ $(CCOPT) $(DEBUG) -I$(AE_DIR) $(LDFLAGS) -lhierrdb example-ae.c $(AE_DIR)/ae.o $(AE_DIR)/zmalloc.o
endif

hierrdb-%: %.o ${DYLIBNAME}
	$(CC) -o $@ $(CCOPT) $(DEBUG) $(LDFLAGS) -lhierrdb $<

test: hierrdb-test
	./hierrdb-test

.c.o:
	$(CC) -c $(CFLAGS) $(OBJARCH) $(DEBUG) $(COMPILE_TIME) $<

clean:
	rm -rf ${DYLIBNAME} ${STLIBNAME} $(BINS) hierrdb-example* *.o *.gcda *.gcno *.gcov

dep:
	$(CC) -MM *.c

install: ${DYLIBNAME} ${STLIBNAME}
	mkdir -p $(INSTALL_INC) $(INSTALL_LIB)
	$(INSTALL) hierrdb.h async.h adapters $(INSTALL_INC)
	$(INSTALL) ${DYLIBNAME} ${STLIBNAME} $(INSTALL_LIB)

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
