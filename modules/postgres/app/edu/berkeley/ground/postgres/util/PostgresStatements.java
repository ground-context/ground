package edu.berkeley.ground.postgres.util;

import edu.berkeley.ground.common.util.DbStatements;
import java.util.ArrayList;
import java.util.List;

public class PostgresStatements implements DbStatements<String> {

  List<String> statements;

  public PostgresStatements() {
    this.statements = new ArrayList<>();
  }

  public PostgresStatements(List<String> statements) {
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
