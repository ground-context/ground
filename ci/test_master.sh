#!/bin/bash

# set environment variables
export COVERALLS_TOKEN=token

# start Postgres
service postgresql start

# create Postgres db and user
su -c "createuser test -d -s" -s /bin/sh postgres
su -c "createdb test" -s /bin/sh postgres

# start Cassandra and sleep to wait for it to start
service cassandra start
sleep 20

# create Cassandra keyspace
cqlsh -e "create keyspace test with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"

# start Neo4j
service neo4j start

rm -rf /tmp/ground/
git clone https://github.com/ground-context/ground
cd ground

# set Postgres and Cassandra schemas
cd ground-core/scripts/postgres && python2.7 postgres_setup.py test test && cd ../../..
cd ground-core/scripts/cassandra && python2.7 cassandra_setup.py test && cd ../../..

# run tests
mvn clean test

# generate the test coverage report and send it to Coveralls 
mvn clean test jacoco:report coveralls:report -DrepoToken=$COVERALLS_TOKEN -Dbranch=master
