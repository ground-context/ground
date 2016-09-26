[![CircleCI](https://circleci.com/gh/ground-context/ground.png?circle-token=4b494a01575da18eecdbbb180b89c2d8bd18cdb0)](https://circleci.com/gh/ground-context/ground-context)
[![Coveralls](https://img.shields.io/coveralls/ground-context/ground.svg)](https://coveralls.io/github/ground-context/ground)


# Ground

Ground is an open-source data context service under development at UC Berkeley. Ground serves as a central model, API, and repistory for capturing the broad context in which data is used. Our goal is to address practical problems for the Big Data community in the short term and to open up opportunities for long-term research and innovation.

For the vision behind Ground as well as a more detailed description of the state of the project, please see our [CIDR '17](http://cidrdb.org/cidr2017/) [submssion](CIDR17.pdf).

## Getting Started

Please take a look at our Getting Started docs. We lead you through an example scenario, enabling Ground as the Hive Metstore and loading metadata across common big data tools (HDFS, Hive, and git).

Once you have completed the Getting Started docs, please take a look at our Next Steps docs.

We'd love to hear any feedback you have!

## Building and Running Ground

If you'd like to build Ground from source:
`mvn clean package -DskipTests`.

To start a Ground server: `java -jar ground-core/target/ground-core-0.1-SNAPSHOT.jar server conf/config.yml`.

## License

Ground is licensed under the [Apache v2
License](http://www.apache.org/licenses/LICENSE-2.0). 

## Contributing

Please see the guidelines in
[CONTRIBUTING.md](https://github.com/ground-context/ground/blob/master/CONTRIBUTING.md).
