# Semantic Web Search Engine (SWSE)

Legacy code. The code was built from the ground up as a distributed system that uses RMI for communication.

The code includes packages for crawling, ranking, reasoning, querying and searching large-scale RDF datasets.

The entry point is the org.semanticweb.swse.cli.* package, which allows for running RMI servers and invoking them.

Another entry point are the test packages, where one can view small test-cases for individual components.

A build.xml allows for building an executable jar; such a jar (compiled with Java 6) is also included in the repository in the dist/ folder. Running this jar gives a list of possible classes to run from the CLI package.
