package com.rpl.rama.helpers.spatial;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.rpl.rama.RamaSerializable;

import clojure.lang.PersistentHashMap;
import clojure.lang.PersistentVector;

// TODO custom read/write
public abstract class Node implements INode, RamaSerializable {
  final long id;
  long parent;
  PersistentVector children;

  public Node(long id, long parent) {
    this.id = id;
    this.parent = parent;
    this.children = Vector.empty();
  }

  /* public Node(long id, long parent, Child child) { */
  /*   this.id = id; */
  /*   this.parent = parent; */
  /*   this.children = new ArrayList<>(Arrays.asList(child)); */
  /* } */

  @Override
  public String toString() {
    return "Node [id=" + id +
      ", parent=" + parent +
      ", children=" + children +
      "]";
  }

  public long count() {
    return children.size();
  }

  public PersistentVector getChildren() {
    return children;
  }

  public Child child(int i) {
    return (Child)children.get(i);
  }

  public int numFreeEdges(int branchingFactor) {
    return branchingFactor - children.size();
  }

  public boolean isFull(int branchingFactor) {
    return branchingFactor == children.size();
  }

  public Node add(MBR bounds, long id) {
    System.out.println(
      "Node::add "+ this + "  bounds: "+ bounds + ",  id: " + id);
    children = children.cons(new Child(bounds, id));
    return this;
  }

  public Node addChild(Child child) {
    children = children.cons(child);
    return this;
  }

  public long nodeId() {
    return id;
  }

  public long parentId() {
    return parent;
  }

  public Node setParentId(long parentId) {
    this.parent = parentId;
    return this;
  }

  public boolean isRoot() {
    return id == parent;
  }

  public List<Long> overlapping(MBR bounds) {
    return ((Collection<Child>)children).stream()
      .filter(child -> child.bounds.isIntersects(bounds))
      .map(child -> child.id)
      .collect(Collectors.toList());
  }

  public MBR unionBounds() {
    MBR unionBounds = ((Child)children.get(0)).bounds;
    for (Child child : (Collection<Child>)children) {
      unionBounds = unionBounds.union(child.bounds);
    }
    return unionBounds;
  }

  public Node updateChild(Node other) {
    for (int i=0; i < children.size() ; i++) {
      Child child = (Child)children.get(i);
      if (child.id == other.nodeId()) {
        children = (PersistentVector)children
          .set(i, new Child(other.bounds(), other.nodeId()));
        break;
      }
    }
    return this;
  }

  public List<Child> extremes()
  // LPSl.[Find extreme rectangles along all dimensions.]

  // Along each dimension, find the entry whose rectangle has the highest low
  // side, and the one with the lowest high side. Record the separation.

  // LPS2. [Adjust for shape of the rectangle cluster.] Normalize the
  // separations by dividing by the width of the entire set along the
  // corresponding dimension.

  // LPS3. [Select the most extreme pair.] Choose the pair with the greatest
  // normalised separation alobg any dimension.
  {
    Child first = (Child)children.get(0);
    int dimensions = first.bounds.dimensions();
    List<Child> lowers = new ArrayList<>(Collections.nCopies(dimensions, first));
    List<Child> uppers = new ArrayList<>(Collections.nCopies(dimensions, first));
    final MBR unionBounds = unionBounds();
    for (Child child : (Collection<Child>)children) {
      for (int dimension = 0; dimension < dimensions; dimension++) {
        if (child.bounds.isHigher(lowers.get(dimension).bounds, dimension)) {
          lowers.set(dimension, child);
        }
        if (child.bounds.isLower(uppers.get(dimension).bounds, dimension)) {
          uppers.set(dimension, child);
        }
      }
    }
    double[] extents = IntStream.range(0, dimensions)
      .mapToDouble(dimension -> unionBounds.getApproximateExtent(dimension))
      .toArray();

    double maxSeparation = -1.0;
    int maxDimension = 0;
    for (int dimension = 0; dimension < dimensions; dimension++) {
      double separation =
        (uppers.get(dimension).bounds.getMin(dimension) -
         lowers.get(dimension).bounds.getMax(dimension)) /
        extents[dimension];
      if (separation > maxSeparation) {
        maxSeparation = separation;
        maxDimension = dimension;
      }
    }

    return Arrays.asList(lowers.get(maxDimension), uppers.get(maxDimension));
  }

  private Child pickNext(List<Child> candidates) {
    // simply chooses any of the remaining entries
    Child child = candidates.get(0);
    candidates.remove(0);
    return child;
  }

  public Node splitNode(long newNodeId, int minChildren) {
    // S1. [Pick first entry for each group.]
    // Apply Algorithm PickSeeds to choose two entries to be the first
    // elements of the groups. Assign each to a group.
    // logger.error("splitNodeImpl");

    List<Child> toInsert = new ArrayList<>((Collection<Child>)children);
    List<Child> seeds = extremes();
    toInsert.removeAll(seeds);

    children = Vector.empty();
    children = children.cons(seeds.get(0));

    List<Node> nodes
      = Arrays.asList(this, ((Node)newSibling(newNodeId)).addChild(seeds.get(1)));

    // S2. [Check if done.] If all entries have been assigned, stop.
    while (!toInsert.isEmpty()) {
      // If one group has so few entries that all the rest must be assigned to
      // it in order for it to have the minimum number m, assign them and
      // stop.
      int nRemaining = toInsert.size();
      if (nRemaining <= minChildren - this.count()) {
        this.children = Vector.into(children, toInsert);
        break;
      } else if (nRemaining <= minChildren - nodes.get(1).count()) {
        Node n = nodes.get(1);
        n.children = n.children.cons(toInsert);
        break;
      }

      // S3. [Select entry to assign.] Invoke Algorithm PickNext to choose the
      // next entry to assign.
      Child next = pickNext(toInsert);

      // Add it to the group whose covering rectangle
      // will have to be enlarged least to accommodate it. Resolve ties by
      // adding the entry to the group with smaller area, then to the one with
      // fewer entries, then to either. Repeat from S2.
      int minGroup = -1;
      double minAreaChange = Double.MAX_VALUE;
      for (int i = 0; i<2; i++) {
        Node candidate = nodes.get(i);
        MBR mbr = candidate.bounds();
        double areaChange =
          mbr.union(next.bounds).area() - mbr.area();
        if (areaChange < minAreaChange) {
          minAreaChange = areaChange;
          minGroup = i;
        }
        // TODO break ties
      }
      nodes.get(minGroup).add(next.bounds, next.id);

    }
    return nodes.get(1);
  }

  public MBR bounds() {
    MBR bounds = ((Child)children.get(0)).bounds;
    for (Child child : (Collection<Child>)children) {
      bounds = bounds.union(child.bounds);
    }
    return bounds;
  }

  /** Return the child whose bounds needs least enlargement to include `bounds`.
      Resolve ties by choosing the entry with the rectangle of smallest area.
    */
  public long chooseChild(MBR bounds) {
    double minDelta = Double.MAX_VALUE;
    double chosenArea = Double.MAX_VALUE;
    long childId = -1;
    for (Child child : (Collection<Child>)children) {
      MBR unionBounds = bounds.union(child.bounds);
      double childArea = child.bounds.area();
      double delta = unionBounds.area() - childArea;
      if (((minDelta == delta) && (childArea < chosenArea)) ||
          minDelta > delta) {
        minDelta = delta;
        chosenArea = childArea;
        childId = child.id;
      }
    }
    return childId;
  }

  public List<String> boundsStrings() {
    return ((Collection<Child>) children)
        .stream()
        .map(Child::boundsString)
        .collect(Collectors.toList());
  }
}
