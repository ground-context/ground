package edu.berkeley.ground.db;

import edu.berkeley.ground.exceptions.GroundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class PostgresResults implements QueryResults {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresResults.class);

    private ResultSet resultSet;

    public PostgresResults(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    public boolean next() throws GroundException {
        try {
            return this.resultSet.next();
        } catch (SQLException e) {
            throw new GroundException(e);
        }
    }

    public String getString(int index) throws GroundException {
        try {
            return resultSet.getString(index);
        } catch (SQLException e) {
            LOGGER.error(e.getMessage());

            throw new GroundException(e);
        }
    }

    public int getInt(int index) throws GroundException {
        try {
            return resultSet.getInt(index);
        } catch (SQLException e) {
            LOGGER.error(e.getMessage());

            throw new GroundException(e);
        }
    }

    public boolean getBoolean(int index) throws GroundException {
        try {
            return resultSet.getBoolean(index);
        } catch (SQLException e) {
            LOGGER.error(e.getMessage());

            throw new GroundException(e);
        }
    }

    public List<String> getStringList() throws GroundException {
        try {
            List<String> stringList = new ArrayList<>();
            do {
                stringList.add(this.getString(2));
            } while (resultSet.next());

            return stringList;
        } catch (SQLException e) {
            LOGGER.error(e.getMessage());

            throw new GroundException(e);
        }
    }
}
