#!/bin/bash

# stop running postgres and remove any existing versions
sudo service postgresql stop
sudo rm -rf /etc/postgresql/
sudo rm -rf /etc/postgresql-common/
sudo rm -rf /var/lib/postgresql/

# install postgres
sudo apt-get install -y postgresql
sudo sed -i "s|peer|trust|g" /etc/postgresql/9.6/main/pg_hba.conf
sudo sed -i "s|md5|trust|g" /etc/postgresql/9.6/main/pg_hba.conf

# stop running cassandra and remove any existing versions
sudo service cassandra stop
sudo rm -rf /var/lib/cassandra
sudo rm -rf /var/log/cassandra
sudo rm -rf /etc/cassandra

# add the apache cassandra information
sudo echo "deb http://www.apache.org/dist/cassandra/debian 39x main" | sudo tee -a /etc/apt/sources.list.d/cassandra.sources.list
sudo echo "deb-src http://www.apache.org/dist/cassandra/debian 39x main" | sudo tee -a /etc/apt/sources.list.d/cassandra.sources.list
gpg --keyserver pgp.mit.edu --recv-keys F758CE318D77295D
gpg --export --armor F758CE318D77295D | sudo apt-key add -
gpg --keyserver pgp.mit.edu --recv-keys 2B5C1B00
gpg --export --armor 2B5C1B00 | sudo apt-key add -
gpg --keyserver pgp.mit.edu --recv-keys 0353B12C
gpg --export --armor 0353B12C | sudo apt-key add -

# install cassandra
sudo apt-get update
sudo apt-get install -y --allow-unauthenticated -o Dpkg::Options::="--force-confnew" cassandra
printf "[cql]\nversion = 3.4.2\n" > ~/.cqlshrc

# install neo4j
sudo wget -O neo4j-community-$NEO4J_VERSION-unix.tar.gz https://neo4j.com/artifact.php?name=neo4j-community-$NEO4J_VERSION-unix.tar.gz
sudo tar -xzf neo4j-community-$NEO4J_VERSION-unix.tar.gz
sudo sed -i "s|#dbms.security.auth_enabled=false|dbms.security.auth_enabled=false|g" neo4j-community-$NEO4J_VERSION/conf/neo4j.conf

# install python and clqsh
sudo apt-get install -y python-pip
sudo pip install cqlsh
