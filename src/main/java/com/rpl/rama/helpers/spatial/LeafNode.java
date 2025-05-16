package com.rpl.rama.helpers.spatial;

import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class LeafNode extends Node {
  /* final long id; */
  /* long parent; */
  /* final List<Child> objects; */

  public LeafNode(long id, long parent) {
    super(id, parent);
    /* this.id = id; */
    /* this.parent = parent; */
    /* this.objects = new ArrayList<>(); */
  }

  @Override
  public String toString() {
    return "Leaf" + super.toString();
  }

  /* public LeafNode(long id, long parent, Child child) { */
  /*   this.id = id; */
  /*   this.parent = parent; */
  /*   this.objects = new ArrayList<>(Arrays.asList(child)); */
  /* } */

  public boolean isLeaf() {
    return true;
  }

  public LeafNode newSibling(long id) {
    return new LeafNode(id, parent);
  }

  /* public long count() { */
  /*   return objects.size(); */
  /* } */

  /* public LeafNode add(MBR bounds, long id) { */
  /*   objects.add(new Child(bounds, id)); */
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
  /*   return objects.stream() */
  /* 	.filter(child -> child.bounds.isIntersects(bounds)) */
  /* 	.map(child -> child.id) */
  /* 	.collect(Collectors.toList()); */
  /* } */

  /* public MBR unionBounds() { */
  /*    MBR unionBounds = objects.get(0).bounds; */
  /*    for (Child child : objects) { */
  /* 	 unionBounds = unionBounds.union(child.bounds); */
  /*    } */
  /*    return unionBounds; */
  /* } */

  /* public List<Child> extremes() */
  /* // LPSl.[Find extreme rectangles along all dimensions.] */

  /* // Along each dimension, find the entry whose rectangle has the highest low */
  /* // side, and the one with the lowest high side. Record the separation. */

  /* // LPS2. [Adjust for shape of the rectangle cluster.] Normalize the */
  /* // separations by dividing by the width of the entire set along the */
  /* // corresponding dimension. */

  /* // LPS3. [Select the most extreme pair.] Choose the pair with the greatest */
  /* // normalised separation alobg any dimension. */
  /* { */
  /*   Child first = objects.get(0); */
  /*   List<Child> lowers = new ArrayList<>(Collections.nCopies(dimensions, first)); */
  /*   List<Child> uppers = new ArrayList<>(Collections.nCopies(dimensions, first)); */
  /*   final MBR unionBounds = unionBounds(); */
  /*   for (Child child : objects) { */
  /* 	for (int dimension = 0; dimension < dimensions; dimension++) { */
  /* 	  if (child.bounds.isHigher(lowers.get(dimension).bounds, dimension)) { */
  /* 	    lowers.set(dimension, child); */
  /* 	  } */
  /* 	   if (child.bounds.isLower(uppers.get(dimension).bounds, dimension)) { */
  /* 	    uppers.set(dimension, child); */
  /* 	  } */
  /* 	} */
  /*   } */
  /*   double[] extents = IntStream.range(0, dimensions) */
  /* 	.mapToDouble(dimension -> unionBounds.getApproximateExtent(dimension)) */
  /* 	.toArray(); */

  /*   double maxSeparation = -1.0; */
  /*   int maxDimension = 0; */
  /*   for (int dimension = 0; dimension < dimensions; dimension++) { */
  /* 	double separation = */
  /* 	  (uppers.get(dimension).bounds.getMin(dimension) - */
  /* 	   lowers.get(dimension).bounds.getMax(dimension)) / */
  /* 	  extents[dimension]; */
  /* 	if (separation > maxSeparation) { */
  /* 	  maxSeparation = separation; */
  /* 	  maxDimension = dimension; */
  /* 	} */
  /*   } */

  /*   return Arrays.asList(lowers.get(maxDimension), uppers.get(maxDimension)); */
  /* } */

  /* private Child pickNext(List<Child> candidates) { */
  /*   // simply chooses any of the remaining entries */
  /*   Child child = candidates.get(0); */
  /*   candidates.remove(0); */
  /*   return child; */
  /* } */

  /* public LeafNode splitNode(long newNodeId) { */
  /*   // S1. [Pick first entry for each group.] */
  /*   // Apply Algorithm PickSeeds to choose two entries to be the first */
  /*   // elements of the groups. Assign each to a group. */
  /*   logger.error("splitNodeImpl"); */

  /*   List<Child> toInsert = new ArrayList<>(objects); */
  /*   List<Child> seeds = extremes(); */
  /*   toInsert.removeAll(seeds); */

  /*   objects.clear(); */
  /*   objects.add(seeds.get(0)); */

  /*   List<LeafNode> nodes */
  /* 	= Arrays.asList(this, new LeafNode(newNodeId, parent, seeds.get(1))); */

  /*   // S2. [Check if done.] If all entries have been assigned, stop. */
  /*   while (!toInsert.isEmpty()) { */
  /* 	// If one group has so few entries that all the rest must be assigned to */
  /* 	// it in order for it to have the minimum number m, assign them and */
  /* 	// stop. */
  /* 	int nRemaining = toInsert.size(); */
  /* 	if (nRemaining <= m - this.count()) { */
  /* 	  this.objects.addAll(toInsert); */
  /* 	  break; */
  /* 	} else if (nRemaining <= m - nodes.get(1).count()) { */
  /* 	  nodes.get(1).objects.addAll(toInsert); */
  /* 	  break; */
  /* 	} */

  /* 	// S3. [Select entry to assign.] Invoke Algorithm PickNext to choose the */
  /* 	// next entry to assign. */
  /* 	Child next = pickNext(toInsert); */

  /* 	// Add it to the group whose covering rectangle */
  /* 	// will have to be enlarged least to accommodate it. Resolve ties by */
  /* 	// adding the entry to the group with smaller area, then to the one with */
  /* 	// fewer entries, then to either. Repeat from S2. */
  /* 	int minGroup = -1; */
  /* 	double minAreaChange = Double.MAX_VALUE; */
  /* 	for (int i = 0; i<2; i++) { */
  /* 	  LeafNode candidate = nodes.get(i); */
  /* 	  MBR mbr = candidate.bounds(); */
  /* 	  double areaChange = */
  /* 	    mbr.union(next.bounds).area() - mbr.area(); */
  /* 	  if (areaChange < minAreaChange) { */
  /* 	    minAreaChange = areaChange; */
  /* 	    minGroup = i; */
  /* 	  } */
  /* 	  // TODO break ties */
  /* 	} */
  /* 	nodes.get(minGroup).add(next.bounds, next.id); */

  /*   } */
  /*   return nodes.get(1); */
  /* } */

  /* public MBR bounds() { */
  /*   MBR bounds = objects.get(0).bounds; */
  /*   for (Child child : objects) { */
  /* 	bounds = bounds.union(child.bounds); */
  /*   } */
  /*   return bounds; */
	/* } */

  public List<String> dotNodes() {
    final String selfNode = "" + id + " [label=\"" + id + "\"]";

    final List<String> objs = children
      .stream()
      .map((Child child) ->
	   "obj_" + child.id + " [label=\"obj_" + child.id + "\"]")
      .collect(Collectors.toList());
    objs.add(selfNode);
    return objs;
  }

  public List<String> dotEdges() {
    return children
      .stream()
      .map((Child child) -> "" + id + " -> obj_" + child.id)
      .collect(Collectors.toList());
  }
}
