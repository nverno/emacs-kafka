SHELL       = /bin/bash
emacs       ?= emacs
batch_flags = -Q -batch
loaddefs    = loaddefs.el
loadpath    = .

el          = $(filter-out $(loaddefs), $(wildcard *.el))
elc         = $(el:.el=.elc)

all: deps install build

build:
	cask build

install:
	cask install

deps:
	@if ! hash cask 2>/dev/null ; then                                            \
          curl -fsSL https://raw.githubusercontent.com/cask/cask/master/go | python ; \
	fi

compile: deps $(elc)
%.elc: %.el
	$(emacs) -L $(loadpath) -L magit-popup $(batch_flags) -f batch-byte-compile $<

# create autoload file
$(loaddefs): deps
	$(emacs) $(batch_flags) -L $(loadpath) $(auto_flags)

clean:
	$(RM) $(elc) $(loaddefs) *~ .#*
