#!/bin/sh

##################################
# Recovery PG database from dump #
##################################

DB_USER=vitrina
DB_PASS=password
DB_NAME=vitrina
DUMP_NAME=dump.sql.gz
eval $(cat .env)
DB_PASS=$PG_DB_PASS

if [ "$#" -eq 1 ]; then
    if [  -e "docker/db/dumps/$1" ]; then
        echo "Restore from dump $1"
        DUMP_NAME=$1
    else
        echo "File does not exists: $1"
        exit 1
    fi
fi

sudo docker-compose exec db sh -c "echo localhost:5432:*:$DB_USER:$DB_PASS > ~/.pgpass && chmod 0600 ~/.pgpass";
sudo docker-compose exec db psql --dbname=$DB_NAME -U $DB_USER -h localhost -p 5432 -c "SELECT pid, pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid();"
sudo docker-compose exec db sh -c "dropdb -U $DB_USER -h localhost -p 5432 $DB_NAME";
sudo docker-compose exec db sh -c "createdb -U $DB_USER -h localhost -p 5432 -O $DB_USER -T template0 $DB_NAME";
sudo docker-compose exec db sh -c "pg_restore -U $DB_USER -h localhost -p 5432 -d $DB_NAME /dumps/$DUMP_NAME";
sudo docker-compose exec db sh -c "rm ~/.pgpass";