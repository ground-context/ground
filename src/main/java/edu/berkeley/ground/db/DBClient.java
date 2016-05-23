package edu.berkeley.ground.db;

import edu.berkeley.ground.exceptions.GroundDBException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface DBClient {
    List<String> SELECT_STAR = Stream.of("*").collect(Collectors.toList());

    GroundDBConnection getConnection() throws GroundDBException;

    abstract class GroundDBConnection {
        public abstract void commit() throws GroundDBException;

        public abstract void abort() throws GroundDBException;
    }
}
