package edu.berkeley.ground.postgres.dao.version.mock;

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.core.RichVersion;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.postgres.dao.core.PostgresRichVersionDao;
import edu.berkeley.ground.postgres.util.PostgresUtils;
import java.util.List;
import play.db.Database;

public class TestPostgresRichVersionDao extends PostgresRichVersionDao<RichVersion> {

  public TestPostgresRichVersionDao(Database dbSource, IdGenerator idGenerator) {
    super(dbSource, idGenerator);
  }

  @Override
  public RichVersion create(RichVersion richVersion, List<Long> parents) throws GroundException {
    PostgresUtils.executeSqlList(this.dbSource, super.insert(richVersion));

    return richVersion;
  }

  @Override
  public Class<RichVersion> getType() {
    return RichVersion.class;
  }
}
