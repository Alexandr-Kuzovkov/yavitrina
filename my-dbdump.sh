#!/bin/sh

###########################
# Store database to  dump #
###########################

DB_USER=vitrina
DB_NAME=vitrina
DB_PASS=password
DUMP_NAME=dump.sql.gz
eval $(cat .env)
DB_PASS=$MYSQL_DB_PASS

case "$1" in
  timestamp)
        timestamp="$(date -Iseconds)"
        DUMP_NAME="$timestamp-$DUMP_NAME"
        echo "Filename of dump: $DUMP_NAME"

    ;;
    esac

sudo docker-compose exec mysql sh -c "mysqldump -u$DB_USER -p$DB_PASS $DB_NAME | gzip > /dumps/$DUMP_NAME"