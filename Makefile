MODULES = pg_lps 
EXTENSION = pg_lps  # the extension's name
DATA = pg_lps--0.0.1.sql    # script file to install
#REGRESS = xxx      # the test script file

# for posgres build
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

#
