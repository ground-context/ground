# Docker Quickstart

You can run these commands to get ground up and running with docker containers

### Run neo4j
`docker run --detach --publish=7474:7474 --publish=7687:7687 --volume=$HOME/neo4j/data:/data neo4j`

You might need to go to http://localhost:7474 and set your password to password

### Building ground docker image
```bash
cd ground/ground-core
mvn clean package docker:build
```
A copy of the config.yml file has to be in the docker folder because docker can't access anything outside of it

### Run Ground
`docker run --detatch --publish=8080:8080 ground-core`

### Run Zookeeper and Kafka
```bash
CID=$(docker run --detach --publish 2181:2181 wurstmeister/zookeeper);
zkip=$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' ${CID});
docker run --detach --volume /var/run/docker.sock:/var/run/docker.sock \
    --env KAFKA_ADVERTISED_HOST_NAME=localhost \
    --env KAFKA_ZOOKEEPER_CONNECT=${zkip}:2181 \
    --publish 9092:9092 wurstmeister/kafka
```
There might be a cleaner way of getting the zookeeper IP

