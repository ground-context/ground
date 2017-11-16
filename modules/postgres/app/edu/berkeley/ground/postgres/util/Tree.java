package edu.berkeley.ground.postgres.util;

public class Tree {

  TreeNode root;
  TreeNode commonRoot;
  TreeNode cwd;

  public Tree( TreeNode root ) {
    this.root = root;
    commonRoot = null;
    cwd = null;
  }

  public void addElement( String elementValue ) {
    String[] list = elementValue.split("/");
    root.addElement(root.incrementalPath, list);

  }

  public void printTree() {
    getCommonRoot();
    commonRoot.printNode(0);
  }

  public TreeNode getCommonRoot() {
    if ( commonRoot != null)
      return commonRoot;
    else {
      TreeNode current = root;
      while ( current.leafs.size() <= 0 ) {
        current = current.children.get(0);
      }
      commonRoot = current;
      return commonRoot;
    }
  }

  public boolean setCwd(String path) {
    String[] listDirectories = path.split("/");
    for (String dir : listDirectories) {
      System.out.println(dir);
    }
    String global = root.data;
    String[] globalDirectory = global.split("/");

    TreeNode currentNode = root;
    for (int i = globalDirectory.length; i < listDirectories.length; i++) {
      String nextDir = listDirectories[i];
      for (int j = 0; j < currentNode.children.size(); j++) {
        TreeNode child = currentNode.children.get(j);
        String dir = child.data;
        if (dir.equals(nextDir)) {
          currentNode = child;
          break;
        } else if (j == currentNode.children.size() - 1) {
          return false;
        }
      }
    }

    cwd = currentNode;
    return true;
  }

  public TreeNode getCwd() {
    if (cwd == null) {
      return root;
    } else {
      return cwd;
    }
  }
}
