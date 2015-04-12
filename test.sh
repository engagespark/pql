#!/bin/bash -e
#
# Description:
#   Creates required test db and runs `go test`
#
# Usage:
#   PGHOST=localhost PGUSER=postgres ./test.sh
#

DBNAME="pql_test"
DBARGS="${DBNAME}"

dropdb $DBARGS || echo "ignored"
createdb $DBARGS
echo "CREATE EXTENSION hstore;" | psql $DBARGS

go test
