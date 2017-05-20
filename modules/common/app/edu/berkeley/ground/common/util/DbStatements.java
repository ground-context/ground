package edu.berkeley.ground.common.util;

import java.util.List;

public interface DbStatements<T> {

  void append(T statement);

  void merge(DbStatements other);

  List<T> getAllStatements();

}
