package edu.berkeley.ground.cassandra.dao.version.mock;

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.core.RichVersion;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.cassandra.dao.core.CassandraRichVersionDao;
import edu.berkeley.ground.cassandra.util.CassandraDatabase;
import edu.berkeley.ground.cassandra.util.CassandraUtils;
import java.util.List;
// import play.db.Database;

public class TestCassandraRichVersionDao extends CassandraRichVersionDao<RichVersion> {

  public TestCassandraRichVersionDao(CassandraDatabase dbSource, IdGenerator idGenerator) {
    super(dbSource, idGenerator);
  }

  @Override
  public RichVersion create(RichVersion richVersion, List<Long> parents) throws GroundException {
    CassandraUtils.executeCqlList(this.dbSource, super.insert(richVersion));

    return richVersion;
  }

  @Override
  public Class<RichVersion> getType() {
    return RichVersion.class;
  }
}
