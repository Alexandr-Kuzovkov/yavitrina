#!/bin/sh

##############################
# Store PG database to  dump #
##############################

DB_USER=vitrina
DB_PASS=password
DB_NAME=vitrina
DUMP_NAME=dump.sql.gz
MAX_STORE_DAYS=30
eval $(cat .env)
DB_PASS=$PG_DB_PASS


case "$1" in
  timestamp)
        timestamp="$(date -Iseconds)"
        DUMP_NAME="$timestamp-$DUMP_NAME"
        echo "Filename of dump: $DUMP_NAME"

    ;;
   removeold)
     echo "Removing files older than $MAX_STORE_DAYS days"
     path="/dumps"
     sudo docker-compose exec db sh -c "find $path -mtime +$MAX_STORE_DAYS -exec rm {} \;"
     sudo docker-compose exec db sh -c "touch $path/.gitkeep"
    esac

sudo docker-compose exec db sh -c "echo localhost:5432:*:$DB_USER:$DB_PASS > ~/.pgpass && chmod 0600 ~/.pgpass";
sudo docker-compose exec db sh -c "pg_dump -Fc -U $DB_USER -h localhost -p 5432 $DB_NAME > /dumps/$DUMP_NAME";
sudo docker-compose exec db sh -c "rm ~/.pgpass";