package edu.berkeley.ground.cassandra.util;

import edu.berkeley.ground.common.util.DbStatements;
import java.util.ArrayList;
import java.util.List;
// import java.util.HashSet; // Andre
// import play.Logger; // Andre - unnecessary

public class CassandraStatements implements DbStatements<String> {

  List<String> statements;
  // HashSet<Long> idsToCreate; // Andre

  public CassandraStatements() {
    this.statements = new ArrayList<>();
    // this.idsToCreate = new HashSet<Long>();
  }

  public CassandraStatements(List<String> statements) {
    this.statements = statements;
  }

  @Override
  public void append(String statement) {
    // Logger.debug("Andre statement append: " + statement);
    this.statements.add(statement);
  }

  // public void intendToCreateId(Long id) { // Andre
  //   this.idsToCreate.add(id);
  // }

  // public boolean isIntendingToCreateId(Long id) { // Andre
  //   return this.idsToCreate.contains(id);
  // }

  @Override
  public void merge(DbStatements other) {
    this.statements.addAll(other.getAllStatements());
  }

  @Override
  public List<String> getAllStatements() {
    return this.statements;
  }
}
