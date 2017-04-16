## Contributing 

Please follow these steps if you are interested in contributing.

* If you are addressing an issue specified in the repo's [issues
list](https://github.com/ground-context/ground/issues), please specifically
explain which issue you are addressing and how you chose to address it.
* If you have found a bug or have an idea for a contribution, please *first*
create an issue explaining the idea *before* opening a pull request to resolve
the issue.
* All code should follow the [Google Java Style
Guide](https://google.github.io/styleguide/javaguide.html). This repository
contains IntelliJ and Eclipse styles based on the Google Java Style Guide.

### Running the test suite

Before submitting a contribution, all existing test must pass and new test covering the added
functionality should be added to the test suite.

The current integration test suite tests Ground against all supported backends. 
For the tests to work, it's required to have a running instance of 
[Apache Cassandra](http://cassandra.apache.org/), [PostgreSQL](https://www.postgresql.org/) 
and [Neo4J](https://neo4j.com/download/).

The test setup can be local to the development machine or use docker containers.
The default configuration assumes that all servers are running in local mode (localhost). 

#### Using a local installation 
The db servers are installed directly on the development machine.  
See [install.sh](/ci/install.sh) for an example on how to setup and configure the db servers. 
 
#### Using Docker images
The backend dependencies can be run using standard docker images of the servers, available 
from [Docker Hub](https://hub.docker.com/).

##### Cassandra: 

```bash
#get the latest version
docker pull cassandra:latest

#run a detached container
docker run -d cassandra:latest
```
##### PostgreSQL

```bash
#get the latest version
docker pull postgres:latest

#run a detached container
docker run -d postgres:latest
```

The postgres server requires setting up a test db and an user
```bash
# We attach to the running container. The container id can be obtained using 'docker ps'
docker exec -i -t <postgres_container_id> /bin/bash

su - postgres
createdb test
createuser test -d -s
exit
exit
```

##### Neo4j
 
```bash
#get the lastest version
docker pull neo4j:latest

#run a detached container
docker run -d neo4j:latest
```

For each container, inspect the assigned ip address for the container and update the corresponding 
host entry in the test configuration:
[test.conf](/src/test/resources/test.conf)

\* Note that pulling the image is only needed the first time.
