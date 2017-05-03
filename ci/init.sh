#!/bin/bash
set -ev

# Create Postgres db and user.
psql -c "CREATE DATABASE test;" -U postgres
psql -c "CREATE ROLE test WITH LOGIN CREATEDB;" -U postgres

# Create Cassandra keyspace.
pip install cqlsh

cqlsh -e "CREATE KEYSPACE test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"

# Set Postgres and Cassandra schemas.
pushd scripts/postgres && python2 postgres_setup.py test test && popd
pushd scripts/cassandra && python2 cassandra_setup.py test && popd
