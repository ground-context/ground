package edu.berkeley.ground.postgres.util;

import edu.berkeley.ground.common.exception.GroundException;

import java.io.FileNotFoundException;

public class SharedFileState {
  private Tree tree;
  public SharedFileState(Tree fileTree) {
    tree = fileTree;
  }
  public Tree getTree() {
    return tree;
  }
  public void setCwd(String path) throws GroundException {
    boolean hasSet = tree.setCwd(path);
    if (!hasSet) {
      throw new GroundException(new FileNotFoundException(path + " does not exist in file tree"));
    }
  }

  public String getCwd() {
    return tree.getCwd().getIncrementalPath();
  }

  public void addFile(String path) {
    tree.addElement(path);
  }
}
