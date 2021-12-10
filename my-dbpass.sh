#!/bin/sh

############################################
# Set password for database from .env file #
############################################

DB_USER=vitrina
DB_NAME=vitrina
DB_PASS_OLD=userp@ssw0rd
DUMP_NAME=dump.sql.gz

if [  -e ".env" ]; then
    eval $(cat .env)
    DB_PASS=$MYSQL_DB_PASS
    sudo docker-compose exec mysql mysql -u$DB_USER -p$DB_PASS_OLD -e "ALTER USER vitrina IDENTIFIED BY '$DB_PASS';"
else
    echo "File .env does not exists!"
    exit 1
fi



