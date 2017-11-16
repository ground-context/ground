package edu.berkeley.ground.postgres.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class TreeNode {

  List<TreeNode> children;
  List<TreeNode> leafs;
  String data;
  String incrementalPath;

  public TreeNode( String nodeValue, String incrementalPath ) {
    children = new ArrayList<TreeNode>();
    leafs = new ArrayList<TreeNode>();
    data = nodeValue;
    this. incrementalPath = incrementalPath;
  }

  public boolean isLeaf() {
    return children.isEmpty() && leafs.isEmpty();
  }

  public void addElement(String currentPath, String[] list) {
    while( list[0] == null || list[0].equals("") )
      list = Arrays.copyOfRange(list, 1, list.length);

    TreeNode currentChild = new TreeNode(list[0], currentPath+"/"+list[0]);
    if ( list.length == 1 ) {
      leafs.add( currentChild );
      return;
    } else {
      int index = children.indexOf( currentChild );
      if ( index == -1 ) {
        children.add( currentChild );
        currentChild.addElement(currentChild.incrementalPath, Arrays.copyOfRange(list, 1, list.length));
      } else {
        TreeNode nextChild = children.get(index);
        nextChild.addElement(currentChild.incrementalPath, Arrays.copyOfRange(list, 1, list.length));
      }
    }
  }

  @Override
  public boolean equals(Object obj) {
    TreeNode cmpObj = (TreeNode)obj;
    return incrementalPath.equals( cmpObj.incrementalPath ) && data.equals( cmpObj.data );
  }

  public void printNode( int increment ) {
    for (int i = 0; i < increment; i++) {
      System.out.print(" ");
    }
    System.out.println(incrementalPath + (isLeaf() ? " -> " + data : "")  );
    for( TreeNode n: children)
      n.printNode(increment+2);
    for( TreeNode n: leafs)
      n.printNode(increment+2);
  }

  public String getIncrementalPath() {
    return incrementalPath;
  }

  @Override
  public String toString() {
    return data;
  }
}

