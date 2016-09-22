[![CircleCI](https://circleci.com/gh/ground-context/ground.png?circle-token=4b494a01575da18eecdbbb180b89c2d8bd18cdb0)](https://circleci.com/gh/ground-context/ground-context)
[![Coveralls](https://img.shields.io/coveralls/ground-context/ground.svg)](https://coveralls.io/github/ground-context/ground)


# Ground

Ground is an open-source data context service under development at UC Berkeley. Ground serves as a central model, API, and repistory for capturing the broad context in which data is used. Our goal is to address practical problems for the Big Data community in the short term and to open up opportunities for long-term research and innovation.

For the vision behind Ground as well as a more detailed description of the state of the project, please see our [CIDR '17](http://cidrdb.org/cidr2017/) submission on [Arxiv](http://arxiv.org).

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
