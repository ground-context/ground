package edu.berkeley.ground.db;

import edu.berkeley.ground.exceptions.GroundDBException;

import java.sql.ResultSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface DBClient {
    List<String> SELECT_STAR = Stream.of("*").collect(Collectors.toList());

    GroundDBConnection getConnection() throws GroundDBException;

    interface GroundDBConnection {
        void insert(String table, List<DbDataContainer> insertValues) throws GroundDBException;

        ResultSet equalitySelect(String table, List<String> projection, List<DbDataContainer> predicatesAndValues) throws GroundDBException;

        void commit() throws GroundDBException;

        void abort() throws GroundDBException;
    }
}
