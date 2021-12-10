#!/bin/sh

############################################
# Set password for database from .env file #
############################################

DB_USER=vitrina
DB_PASS=password
DB_NAME=vitrina
DUMP_NAME=dump.sql.gz
DB_PASS_OLD=P@ssw0rd

if [  -e ".env" ]; then
    eval $(cat .env)
    DB_PASS=$PG_DB_PASS
    sudo docker-compose exec db sh -c "echo localhost:5432:*:$DB_USER:$DB_PASS_OLD > ~/.pgpass && chmod 0600 ~/.pgpass";
    sudo docker-compose exec db psql --dbname=$DB_NAME -U $DB_USER -h localhost -p 5432 -c "ALTER ROLE $DB_USER WITH PASSWORD '$DB_PASS';"
else
    echo "File .env does not exists!"
    exit 1
fi
