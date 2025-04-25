package com.rpl.rama.helpers.spatial;

public interface INode {
  boolean isLeaf();
  INode newSibling(long id);
}
