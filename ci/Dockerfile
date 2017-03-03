FROM ubuntu:16.04

MAINTAINER Vikram Sreekanti <vsreekanti@gmail.com> version: 0.1

RUN apt-get update

# install java 8
RUN apt-get upgrade -y && \
    apt-get install -y  software-properties-common && \
    add-apt-repository ppa:webupd8team/java -y && \
    apt-get update && \
    echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections && \
    apt-get install -y oracle-java8-installer && \
    apt-get clean

# install maven
RUN apt-get install -y maven

# install python2
RUN apt-get install -y python2.7

# install pip & cqlsh
RUN apt-get install -y python-pip
RUN pip install cqlsh

# install and set up postgres
RUN apt-get install -y postgresql
RUN sed 's/peer/trust/g' /etc/postgresql/9.5/main/pg_hba.conf > test.out
RUN sed 's/md5/trust/g' test.out > test2.out
RUN mv test2.out /etc/postgresql/9.5/main/pg_hba.conf 
RUN rm test.out

# install Cassandra
RUN echo "deb http://www.apache.org/dist/cassandra/debian 39x main" | tee -a /etc/apt/sources.list.d/cassandra.sources.list
RUN echo "deb-src http://www.apache.org/dist/cassandra/debian 39x main" | tee -a /etc/apt/sources.list.d/cassandra.sources.list
RUN gpg --keyserver pgp.mit.edu --recv-keys F758CE318D77295D
RUN gpg --export --armor F758CE318D77295D | apt-key add -
RUN gpg --keyserver pgp.mit.edu --recv-keys 2B5C1B00
RUN gpg --export --armor 2B5C1B00 | apt-key add -
RUN gpg --keyserver pgp.mit.edu --recv-keys 0353B12C
RUN gpg --export --armor 0353B12C | apt-key add -
RUN apt-get update
RUN apt-get install -y --allow-unauthenticated cassandra
RUN printf "[cql]\nversion = 3.4.2\n" > ~/.cqlshrc

# download Neo4j
RUN wget --no-check-certificate -O - https://debian.neo4j.org/neotechnology.gpg.key | apt-key add -
RUN echo 'deb http://debian.neo4j.org/repo stable/' >/etc/apt/sources.list.d/neo4j.list
RUN apt-get update
RUN apt-get install -y neo4j
RUN sed -i "s|#dbms.security.auth_enabled=false|dbms.security.auth_enabled=false|g" /etc/neo4j/neo4j.conf
RUN sed -i "s|#dbms.shell|dbms.shell|g" /etc/neo4j/neo4j.conf

# install git
RUN apt-get install -y git

ADD ground/ /tmp/ground

# add test running script for CMD
ADD ground/ci/start_servers_and_test.sh /tmp/

# run the actual tests
CMD sh -e /tmp/start_servers_and_test.sh
