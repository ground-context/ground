/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.ground;

import edu.berkeley.ground.api.models.*;
import edu.berkeley.ground.api.usage.LineageEdgeFactory;
import edu.berkeley.ground.api.usage.LineageEdgeVersionFactory;
import edu.berkeley.ground.db.CassandraClient;
import edu.berkeley.ground.db.Neo4jClient;
import edu.berkeley.ground.db.PostgresClient;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.resources.*;
import edu.berkeley.ground.util.CassandraFactories;
import edu.berkeley.ground.util.Neo4jFactories;
import edu.berkeley.ground.util.PostgresFactories;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.federecio.dropwizard.swagger.SwaggerBundle;
import io.federecio.dropwizard.swagger.SwaggerBundleConfiguration;

public class GroundServer extends Application<GroundServerConfiguration> {
  private EdgeFactory edgeFactory;
  private EdgeVersionFactory edgeVersionFactory;
  private GraphFactory graphFactory;
  private GraphVersionFactory graphVersionFactory;
  private LineageEdgeFactory lineageEdgeFactory;
  private LineageEdgeVersionFactory lineageEdgeVersionFactory;
  private NodeFactory nodeFactory;
  private NodeVersionFactory nodeVersionFactory;
  private StructureFactory structureFactory;
  private StructureVersionFactory structureVersionFactory;

  public static void main(String[] args) throws Exception {
    new GroundServer().run(args);
  }

  @Override
  public String getName() {
    return "ground-server";
  }


  @Override
  public void initialize(Bootstrap<GroundServerConfiguration> bootstrap) {
    bootstrap.addBundle(new SwaggerBundle<GroundServerConfiguration>() {
      @Override
      protected SwaggerBundleConfiguration getSwaggerBundleConfiguration(GroundServerConfiguration configuration) {
        return configuration.swaggerBundleConfiguration;
      }
    });
  }


  @Override
  public void run(GroundServerConfiguration configuration, Environment environment) throws GroundException {
    switch (configuration.getDbType()) {
      case "postgres":
        PostgresClient postgresClient = new PostgresClient(configuration.getDbHost(), configuration.getDbPort(), configuration.getDbName(), configuration.getDbUser(), configuration.getDbPassword());
        setPostgresFactories(postgresClient, configuration.getMachineId(), configuration.getNumMachines());
        break;

      case "cassandra":
        CassandraClient cassandraClient = new CassandraClient(configuration.getDbHost(), configuration.getDbPort(), configuration.getDbName(), configuration.getDbUser(), configuration.getDbPassword());
        setCassandraFactories(cassandraClient, configuration.getMachineId(), configuration.getNumMachines());
        break;

      case "neo4j":
        Neo4jClient neo4jClient = new Neo4jClient(configuration.getDbHost(), configuration.getDbUser(), configuration.getDbPassword());
        setNeo4jFactories(neo4jClient, configuration.getMachineId(), configuration.getNumMachines());
        break;

      default:
        throw new RuntimeException("FATAL: Unrecognized database type (" + configuration.getDbType() + ").");
    }

    final EdgesResource edgesResource = new EdgesResource(edgeFactory, edgeVersionFactory);
    final GraphsResource graphsResource = new GraphsResource(graphFactory, graphVersionFactory);
    final LineageEdgesResource lineageEdgesResource = new LineageEdgesResource(lineageEdgeFactory, lineageEdgeVersionFactory);
    final NodesResource nodesResource = new NodesResource(nodeFactory, nodeVersionFactory);
    final StructuresResource structuresResource = new StructuresResource(structureFactory, structureVersionFactory);
    final KafkaResource kafkaResource = new KafkaResource(configuration.getKafkaHost(), configuration.getKafkaPort());

    environment.jersey().register(edgesResource);
    environment.jersey().register(graphsResource);
    environment.jersey().register(lineageEdgesResource);
    environment.jersey().register(nodesResource);
    environment.jersey().register(structuresResource);
    environment.jersey().register(kafkaResource);
  }

  private void setPostgresFactories(PostgresClient postgresClient, int machineId, int numMachines) {
    PostgresFactories factoryGenerator = new PostgresFactories(postgresClient, machineId, numMachines);

    edgeFactory = factoryGenerator.getEdgeFactory();
    edgeVersionFactory = factoryGenerator.getEdgeVersionFactory();
    graphFactory = factoryGenerator.getGraphFactory();
    graphVersionFactory = factoryGenerator.getGraphVersionFactory();
    lineageEdgeFactory = factoryGenerator.getLineageEdgeFactory();
    lineageEdgeVersionFactory = factoryGenerator.getLineageEdgeVersionFactory();
    nodeFactory = factoryGenerator.getNodeFactory();
    nodeVersionFactory = factoryGenerator.getNodeVersionFactory();
    structureFactory = factoryGenerator.getStructureFactory();
    structureVersionFactory = factoryGenerator.getStructureVersionFactory();
  }

  private void setCassandraFactories(CassandraClient cassandraClient, int machineId, int numMachines) {
    CassandraFactories factoryGenerator = new CassandraFactories(cassandraClient, machineId, numMachines);

    edgeFactory = factoryGenerator.getEdgeFactory();
    edgeVersionFactory = factoryGenerator.getEdgeVersionFactory();
    graphFactory = factoryGenerator.getGraphFactory();
    graphVersionFactory = factoryGenerator.getGraphVersionFactory();
    lineageEdgeFactory = factoryGenerator.getLineageEdgeFactory();
    lineageEdgeVersionFactory = factoryGenerator.getLineageEdgeVersionFactory();
    nodeFactory = factoryGenerator.getNodeFactory();
    nodeVersionFactory = factoryGenerator.getNodeVersionFactory();
    structureFactory = factoryGenerator.getStructureFactory();
    structureVersionFactory = factoryGenerator.getStructureVersionFactory();
  }

  private void setNeo4jFactories(Neo4jClient neo4jClient, int machineId, int numMachines) {
    Neo4jFactories factoryGenerator = new Neo4jFactories(neo4jClient, machineId, numMachines);

    edgeFactory = factoryGenerator.getEdgeFactory();
    edgeVersionFactory = factoryGenerator.getEdgeVersionFactory();
    graphFactory = factoryGenerator.getGraphFactory();
    graphVersionFactory = factoryGenerator.getGraphVersionFactory();
    lineageEdgeFactory = factoryGenerator.getLineageEdgeFactory();
    lineageEdgeVersionFactory = factoryGenerator.getLineageEdgeVersionFactory();
    nodeFactory = factoryGenerator.getNodeFactory();
    nodeVersionFactory = factoryGenerator.getNodeVersionFactory();
    structureFactory = factoryGenerator.getStructureFactory();
    structureVersionFactory = factoryGenerator.getStructureVersionFactory();
  }
}
