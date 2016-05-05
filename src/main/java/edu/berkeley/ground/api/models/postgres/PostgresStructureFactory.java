package edu.berkeley.ground.api.models.postgres;

import edu.berkeley.ground.api.models.Structure;
import edu.berkeley.ground.api.models.StructureFactory;
import edu.berkeley.ground.api.versions.Type;
import edu.berkeley.ground.api.versions.postgres.PostgresItemFactory;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class PostgresStructureFactory extends StructureFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(Structure.class);

    private PostgresItemFactory itemFactory;

    public PostgresStructureFactory(PostgresItemFactory itemFactory) {
        this.itemFactory = itemFactory;
    }
    public Structure create(GroundDBConnection connection, String name) throws GroundException {
        String uniqueId = "Structures." + name;

        this.itemFactory.insertIntoDatabase(connection, uniqueId);

        List<DbDataContainer> insertions = new ArrayList<>();
        insertions.add(new DbDataContainer("name", Type.STRING, name));
        insertions.add(new DbDataContainer("item_id", Type.STRING, uniqueId));

        connection.insert("Structures", insertions);

        LOGGER.info("Created structure " + name + ".");

        return StructureFactory.construct(uniqueId, name);
    }

    public Structure retrieveFromDatabase(GroundDBConnection connection, String name)  throws GroundException {
        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("name", Type.STRING, name));

        ResultSet resultSet = connection.equalitySelect("Structures", DBClient.SELECT_STAR, predicates);
        String id = DbUtils.getString(resultSet, 1);

        LOGGER.info("Retrieved structure " + name + ".");

        return StructureFactory.construct(id, name);
    }

    public void update(GroundDBConnection connection, String itemId, String childId, Optional<String> parent) throws GroundException {
        this.itemFactory.update(connection, itemId, childId, parent);
    }
}
