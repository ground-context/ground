package edu.berkeley.ground.postgres.utils;

import edu.berkeley.ground.common.utils.DbStatements;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by hdy332 on 5/11/17.
 */
public class PostgresStatements implements DbStatements<String> {

  List<String> statements;

  public PostgresStatements() {
    this.statements = new ArrayList<>();
  }

  public PostgresStatements(List<String> statements) {
    this.statements = statements;
  }

  public void append(String statement) {
    this.statements.add(statement);
  }

  public void merge(DbStatements other) {
    this.statements.addAll(other.getAllStatements());
  }

  public List<String> getAllStatements() {
    return this.statements;
  }
}
