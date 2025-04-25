package com.rpl.rama.helpers.spatial;

/** A node in the R-Tree */
public class NonLeafNode extends Node {
  /* long id; */
  /* long parent; */
  /* List<Child> children; */

  public NonLeafNode(long id, long parent) {
    super(id, parent);
  }

  public boolean isLeaf() {
    return false;
  }

  public NonLeafNode newSibling(long id) {
    return new NonLeafNode(id, parent);
  }

  /* public long count() { */
  /*   return children.size(); */
  /* } */

  /* public NonLeafNode add(MBR bounds, long id) { */
  /*   children.add(new Child(bounds, id)); */
  /*   return this; */
  /* } */

  /* public long nodeId() { */
  /*   return id; */
  /* } */

  /* public long parentId() { */
  /*   return parent; */
  /* } */

  /* public boolean isRoot() { */
  /*   return id == parent; */
  /* } */

  /* public List<Long> overlapping(MBR bounds) { */
  /*   return children.stream() */
  /* 	.filter(child -> child.bounds.isIntersects(bounds)) */
  /* 	.map(child -> child.id) */
  /* 	.collect(Collectors.toList()); */
  /* } */

  /* public MBR bounds() { */
  /*   MBR bounds = children.get(0).bounds; */
  /*   for (Child child : children) { */
  /* 	bounds = bounds.union(child.bounds); */
  /*   } */
  /*   return bounds; */
  /* } */

  /* public Node updateChild(Node other) { */
  /*   for (int i=0; i < children.size() ; i++) { */
  /* 	Child child = children.get(i); */
  /* 	if (child.id == other.nodeId()) { */
  /* 	  children.set(i, new Child(other.bounds(), other.nodeId())); */
  /* 	  break; */
  /* 	} */
  /*   } */
  /*   return this; */
  /* } */
}
