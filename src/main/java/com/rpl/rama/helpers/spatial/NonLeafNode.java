package com.rpl.rama.helpers.spatial;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/** A node in the R-Tree */
public class NonLeafNode extends Node {
  /* long id; */
  /* long parent; */
  /* List<Child> children; */

  public NonLeafNode(Long id, Long parent) {
    super(id, parent);
  }

  public NonLeafNode(long parent) {
    super(parent);
  }

  @Override
  public String toString() {
    return "NonLeaf" + super.toString();
  }

  public boolean isLeaf() {
    return false;
  }

  public NonLeafNode newSibling(long id) {
    return new NonLeafNode(id, parent);
  }

  public NonLeafNode newSibling() {
    return new NonLeafNode(null, parent);
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

  public List<String> dotNodes() {
    return (List<String>) Arrays.asList("" + id + " [label=\"" + id + "\"]");
  }

  public List<String> dotEdges() {
    return ((Collection<Child>)children)
      .stream()
      .map((Child child) ->
           "" + id + " -> " + child.id
           + " [label=\"" + child.bounds.ranges()+"\"]")
      .collect(Collectors.toList());
  }

}
