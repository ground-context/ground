# Ground

[![Build Status](https://travis-ci.org/ground-context/ground.svg?branch=master)](https://travis-ci.org/ground-context/ground)
[![codecov](https://codecov.io/gh/ground-context/ground/branch/master/graph/badge.svg)](https://codecov.io/gh/ground-context/ground)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Ground is an open-source data context service under development in UC Berkeley's [RISE Lab](https://rise.cs.berkeley.edu/). Ground serves as a central model, API, and repository for capturing the broad context in which data is used. Our goal is to address practical problems for the Big Data community in the short term and to open up opportunities for long-term research and innovation.

For the vision behind Ground as well as a more detailed description of the state of the project, please see our [CIDR '17](http://cidrdb.org/cidr2017/) [publication](docs/CIDR17.pdf).

## Getting Started

Please take a look at our [Getting Started](https://github.com/ground-context/ground/wiki/Getting-Started) docs. We lead you through an example scenario, enabling Ground as the Hive Metastore and loading metadata across common big data tools (HDFS, Hive, and git).

Once you have completed the Getting Started docs, please take a look at our Next Steps docs.

We'd love to hear any feedback you have!

## Building and Running Ground

If you'd like to build Ground from source:
`mvn clean package -DskipTests`.

To start a Ground server: `java -jar target/ground-0.1-SNAPSHOT.jar server conf/config.yml`.

## License

Ground is licensed under the [Apache v2 License](https://www.apache.org/licenses/LICENSE-2.0).

## Contributing

Please see the guidelines in [CONTRIBUTING.md](CONTRIBUTING.md).
