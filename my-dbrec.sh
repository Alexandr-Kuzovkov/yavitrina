#!/bin/sh

###############################
# Recovery database from dump #
###############################

DB_USER=vitrina
DB_PASS=foBCKFduY5
DB_NAME=vitrina
DUMP_NAME=dump.sql.gz

if [ "$#" -eq 1 ]; then
    if [  -e "docker/mysql/dumps/$1" ]; then
        echo "Restore from dump $1"
        DUMP_NAME=$1
    else
        echo "File does not exists: $1"
        exit 1
    fi
fi

sudo docker-compose exec mysql mysql -u$DB_USER -p$DB_PASS -e "DROP DATABASE IF EXISTS $DB_NAME"
sudo docker-compose exec mysql mysql -u$DB_USER -p$DB_PASS -e "CREATE DATABASE IF NOT EXISTS $DB_NAME"
sudo docker-compose exec mysql sh -c "gunzip < /dumps/$DUMP_NAME | mysql -u$DB_USER -p$DB_PASS $DB_NAME"

