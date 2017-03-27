#!/bin/bash

# start postgres
sudo service postgresql start
sudo service postgresql status
sudo service postgresql restart

# create Postgres db and user
sudo su -c "createdb test" -s /bin/sh postgres
sudo su -c "createuser test -d -s" -s /bin/sh postgres

# start cassandra
sudo chmod 750 /var/run/cassandra
sudo service cassandra start
sleep 20

# create Cassandra keyspace
cqlsh -e "create keyspace test with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"

# start Neo4j
sudo neo4j-community-$NEO4J_VERSION/bin/neo4j start

# set Postgres and Cassandra schemas
cd scripts/postgres && python2.7 postgres_setup.py test test && cd ../..
cd scripts/cassandra && python2.7 cassandra_setup.py test && cd ../..
