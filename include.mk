## -*- makefile -*-

## Erlang
ROOT := ../

ERL := erl
ERLC := $(ERL)c

INCLUDE_DIRS := $(ROOT)/include 
ERLC_FLAGS := -W $(INCLUDE_DIRS:$(ROOT)/%=-I $(ROOT)/%) 

ifndef no_debug_info
  ERLC_FLAGS += +debug_info
endif

ifdef debug
  ERLC_FLAGS += -Ddebug
endif

EBIN_DIR := $(ROOT)/ebin
DOC_DIR  := $(ROOT)/doc
EMULATOR := beam

ERL_SOURCES := $(wildcard *.erl)
ERL_HEADERS := $(wildcard *.hrl) $(wildcard $(ROOT)/include/*.hrl)
ERL_OBJECTS := $(ERL_SOURCES:%.erl=$(EBIN_DIR)/%.$(EMULATOR))
ERL_OBJECTS_LOCAL := $(ERL_SOURCES:%.erl=./%.$(EMULATOR))
APP_FILES := $(wildcard *.app)
EBIN_FILES = $(ERL_OBJECTS) $(APP_FILES:%.app=$(ROOT)/ebin/%.app)
EBIN_FILES_NO_DOCS = $(ERL_OBJECTS) $(APP_FILES:%.app=$(ROOT)/ebin/%.app)
MODULES = $(ERL_SOURCES:%.erl=%)

$(ROOT)/ebin/%.app: %.app
	cp $< $@

$(EBIN_DIR)/%.$(EMULATOR): %.erl
	$(ERLC) $(ERLC_FLAGS) -o $(EBIN_DIR) $<

./%.$(EMULATOR): %.erl
	$(ERLC) $(ERLC_FLAGS) -o . $<
