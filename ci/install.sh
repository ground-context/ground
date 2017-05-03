#!/bin/bash
set -ev

# Add Apache Cassandra to sources.
echo "deb http://www.apache.org/dist/cassandra/debian 310x main" | sudo tee -a /etc/apt/sources.list.d/cassandra.sources.list
curl https://www.apache.org/dist/cassandra/KEYS | sudo apt-key add -

# Add Neo4j to sources.
echo "deb https://debian.neo4j.org/repo stable/" | sudo tee -a /etc/apt/sources.list.d/neo4j.list
curl https://debian.neo4j.org/neotechnology.gpg.key | sudo apt-key add -

# Install Cassandra and Neo4j.
sudo apt-get -qq update
sudo apt-get -y install cassandra neo4j

# Force cqlsh to use the right version.
printf "[cql]\nversion = 3.4.4\n" > ~/.cqlshrc

# Disable auth for Neo4j.
sudo sed -i "s/#dbms\.security\.auth_enabled=false/dbms\.security\.auth_enabled=false/g" /etc/neo4j/neo4j.conf
sudo service neo4j restart
