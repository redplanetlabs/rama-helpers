package com.rpl.rama.helpers.spatial;

import java.util.List;

public interface INode {
  boolean isLeaf();
  INode newSibling(long id);
  INode newSibling();

  public List<String> dotNodes();
  public List<String> dotEdges();
}
