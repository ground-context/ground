#!/bin/bash

# start postgres
sudo service postgresql start
sudo service postgresql status
sudo service postgresql restart

echo "restarted postgres"

# create Postgres db and user
sudo su -c "createdb test" -s /bin/bash postgres
sudo su -c "createuser test -d -s" -s /bin/bash postgres

echo "created dbs"

cd modules/postgres/dist/db && python2.7 postgres_setup.py test test && cd ../../../..
