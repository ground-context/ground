package edu.berkeley.ground.postgres.util;

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.postgres.dao.core.PostgresEdgeDao;
import edu.berkeley.ground.postgres.dao.core.PostgresEdgeVersionDao;
import edu.berkeley.ground.postgres.dao.core.PostgresGraphDao;
import edu.berkeley.ground.postgres.dao.core.PostgresGraphVersionDao;
import edu.berkeley.ground.postgres.dao.core.PostgresNodeDao;
import edu.berkeley.ground.postgres.dao.core.PostgresNodeVersionDao;
import edu.berkeley.ground.postgres.dao.core.PostgresStructureDao;
import edu.berkeley.ground.postgres.dao.core.PostgresStructureVersionDao;
import edu.berkeley.ground.postgres.dao.usage.PostgresLineageEdgeDao;
import edu.berkeley.ground.postgres.dao.usage.PostgresLineageEdgeVersionDao;
import edu.berkeley.ground.postgres.dao.usage.PostgresLineageGraphDao;
import edu.berkeley.ground.postgres.dao.usage.PostgresLineageGraphVersionDao;
import edu.berkeley.ground.postgres.dao.version.PostgresTagDao;
import edu.berkeley.ground.postgres.dao.version.PostgresVersionHistoryDagDao;
import edu.berkeley.ground.postgres.dao.version.PostgresVersionSuccessorDao;
import javax.inject.Inject;
import javax.inject.Singleton;
import play.db.Database;

@Singleton
public class Daos {

  private final PostgresStructureDao postgresStructureDao;
  private final PostgresStructureVersionDao postgresStructureVersionDao;
  private final PostgresEdgeDao postgresEdgeDao;
  private final PostgresEdgeVersionDao postgresEdgeVersionDao;
  private final PostgresGraphDao postgresGraphDao;
  private final PostgresGraphVersionDao postgresGraphVersionDao;
  private final PostgresNodeDao postgresNodeDao;
  private final PostgresNodeVersionDao postgresNodeVersionDao;

  private final PostgresLineageEdgeDao postgresLineageEdgeDao;
  private final PostgresLineageEdgeVersionDao postgresLineageEdgeVersionDao;
  private final PostgresLineageGraphDao postgresLineageGraphDao;
  private final PostgresLineageGraphVersionDao postgresLineageGraphVersionDao;

  @Inject
  public Daos(Database dbSource, IdGenerator idGenerator) throws GroundException {

    PostgresVersionSuccessorDao postgresVersionSuccessorDao = new PostgresVersionSuccessorDao(dbSource, idGenerator);
    PostgresVersionHistoryDagDao postgresVersionHistoryDagDao = new PostgresVersionHistoryDagDao(dbSource,
                                                                                                  postgresVersionSuccessorDao);
    PostgresTagDao postgresTagDao = new PostgresTagDao(dbSource);

    this.postgresStructureDao = new PostgresStructureDao(dbSource, idGenerator);
    this.postgresStructureVersionDao = new PostgresStructureVersionDao(dbSource, idGenerator);
    this.postgresEdgeDao = new PostgresEdgeDao(dbSource, idGenerator);
    this.postgresEdgeVersionDao = new PostgresEdgeVersionDao(dbSource, idGenerator);
    this.postgresGraphDao = new PostgresGraphDao(dbSource, idGenerator);
    this.postgresGraphVersionDao = new PostgresGraphVersionDao(dbSource, idGenerator);
    this.postgresNodeDao = new PostgresNodeDao(dbSource, idGenerator);
    this.postgresNodeVersionDao = new PostgresNodeVersionDao(dbSource, idGenerator);

    this.postgresLineageEdgeDao = new PostgresLineageEdgeDao(dbSource, idGenerator);
    this.postgresLineageEdgeVersionDao = new PostgresLineageEdgeVersionDao(dbSource, idGenerator);
    this.postgresLineageGraphDao = new PostgresLineageGraphDao(dbSource, idGenerator);
    this.postgresLineageGraphVersionDao = new PostgresLineageGraphVersionDao(dbSource, idGenerator);
  }

  public PostgresEdgeDao getPostgresEdgeDao() {
    return this.postgresEdgeDao;
  }

  public PostgresEdgeVersionDao getPostgresEdgeVersionDao() {
    return this.postgresEdgeVersionDao;
  }

  public PostgresGraphDao getPostgresGraphDao() {
    return this.postgresGraphDao;
  }

  public PostgresGraphVersionDao getPostgresGraphVersionDao() {
    return this.postgresGraphVersionDao;
  }

  public PostgresNodeDao getPostgresNodeDao() {
    return this.postgresNodeDao;
  }

  public PostgresNodeVersionDao getPostgresNodeVersionDao() {
    return this.postgresNodeVersionDao;
  }

  public PostgresLineageEdgeDao getPostgresLineageEdgeDao() {
    return this.postgresLineageEdgeDao;
  }

  public PostgresLineageEdgeVersionDao getPostgresLineageEdgeVersionDao() {
    return this.postgresLineageEdgeVersionDao;
  }

  public PostgresStructureDao getPostgresStructureDao() {
    return this.postgresStructureDao;
  }

  public PostgresStructureVersionDao getPostgresStructureVersionDao() {
    return this.postgresStructureVersionDao;
  }

  public PostgresLineageGraphDao getPostgresLineageGraphDao() {
    return this.postgresLineageGraphDao;
  }

  public PostgresLineageGraphVersionDao getPostgresLineageGraphVersionDao() {
    return this.postgresLineageGraphVersionDao;
  }
}
