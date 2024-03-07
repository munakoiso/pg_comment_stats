EXTENSION    = pg_comment_stats
EXTVERSION   = $(shell grep default_version $(EXTENSION).control | sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")
TESTS        = $(wildcard test/sql/*.sql)
REGRESS      = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test

PG_CONFIG ?= pg_config

MODULE_big = pg_comment_stats
OBJS = pg_comment_stats.o pg_time_buffer/pg_time_buffer.o

submodules:
	git submodule update --init pg_stat_kcache
	git submodule update --init pg_time_buffer

all: submodules

release-zip: all
	git archive --format zip --prefix=pg_comment_stats-${EXTVERSION}/ --output ./pg_comment_stats-${EXTVERSION}.zip HEAD
	unzip ./pg_comment_stats-$(EXTVERSION).zip
	rm ./pg_comment_stats-$(EXTVERSION).zip
	rm ./pg_comment_stats-$(EXTVERSION)/.gitignore
	sed -i -e "s/__VERSION__/$(EXTVERSION)/g"  ./pg_comment_stats-$(EXTVERSION)/META.json
	zip -r ./pg_comment_stats-$(EXTVERSION).zip ./pg_comment_stats-$(EXTVERSION)/
	rm ./pg_comment_stats-$(EXTVERSION) -rf


DATA = $(wildcard *--*.sql)
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
