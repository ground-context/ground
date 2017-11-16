package edu.berkeley.ground.postgres.controllers;

import akka.actor.ActorSystem;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.postgres.util.SharedFileState;
import edu.berkeley.ground.postgres.util.Tree;
import edu.berkeley.ground.postgres.util.TreeNode;
import play.cache.CacheApi;
import play.db.Database;

import javax.inject.Inject;

public class FileController {
  private CacheApi cache;
  private ActorSystem actorSystem;
  private String global;
  private SharedFileState sharedFileState;


  @Inject
  final void injectUtils(final CacheApi cache, final Database dbSource, final String global, final String fileLeaf) {
    this.actorSystem = actorSystem;
    this.cache = cache;
    this.global = global;
    sharedFileState = new SharedFileState( new Tree(
      new TreeNode(global, global))); // represents global directory
  }

  public SharedFileState getSharedFileState() {
    return sharedFileState;
  }

  public void setCwd(String path) throws GroundException {
    sharedFileState.setCwd(path);
  }

  public String getCwd(String path) {
    return sharedFileState.getCwd();
  }

  public void addFile(String path) {
    sharedFileState.addFile(path);
  }

}
