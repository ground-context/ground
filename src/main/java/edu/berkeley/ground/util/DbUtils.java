package edu.berkeley.ground.util;

import edu.berkeley.ground.exceptions.GroundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DbUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(DbUtils.class);

    public static String getString(ResultSet resultSet, int index) throws GroundException {
        try {
            return resultSet.getString(index);
        } catch (SQLException e) {
            LOGGER.error(e.getMessage());

            throw new GroundException(e);
        }
    }

    public static int getInt(ResultSet resultSet, int index) throws GroundException {
        try {
            return resultSet.getInt(index);
        } catch (SQLException e) {
            LOGGER.error(e.getMessage());

            throw new GroundException(e);
        }
    }

    public static List<String> getAllStrings(ResultSet resultSet, int index) throws GroundException{
        try {
            List<String> stringList = new ArrayList<>();
            do {
                stringList.add(DbUtils.getString(resultSet, 2));
            } while (resultSet.next());

            return stringList;
        } catch (SQLException e) {
            LOGGER.error(e.getMessage());

            throw new GroundException(e);
        }
    }
}
