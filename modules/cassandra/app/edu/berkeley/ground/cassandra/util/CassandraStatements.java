package edu.berkeley.ground.cassandra.util;

import edu.berkeley.ground.common.util.DbStatements;
import java.util.ArrayList;
import java.util.List;

public class CassandraStatements implements DbStatements<String> {

  List<String> statements;

  public CassandraStatements() {
    this.statements = new ArrayList<>();
  }

  public CassandraStatements(List<String> statements) {
    this.statements = statements;
  }

  @Override
  public void append(String statement) {
    this.statements.add(statement);
  }

  @Override
  public void merge(DbStatements other) {
    this.statements.addAll(other.getAllStatements());
  }

  @Override
  public List<String> getAllStatements() {
    return this.statements;
  }
}
