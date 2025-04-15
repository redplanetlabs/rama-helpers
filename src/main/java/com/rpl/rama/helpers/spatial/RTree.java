package com.rpl.rama.helpers.spatial;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.rpl.rama.Agg;
import com.rpl.rama.Block;
import com.rpl.rama.Expr;
import com.rpl.rama.Helpers;
import com.rpl.rama.PState;
import com.rpl.rama.Path;
import com.rpl.rama.RamaSerializable;
import com.rpl.rama.RamaModule.Topologies;
import com.rpl.rama.helpers.ModuleUniqueIdPState;
import com.rpl.rama.module.ETLTopologyBase;
import com.rpl.rama.ops.Ops;

public class RTree <T> implements RamaSerializable {
  private final String pstatePrefix;
  private final int dimensions;
  private final ModuleUniqueIdPState objectId;
  private final ModuleUniqueIdPState nodeId;
  private final String objectsPstate;
  private final String nodesPstate;
  private final String rootPstate;

  private final int M;
  private final int m;

  public interface Node extends RamaSerializable {
    boolean isLeaf();
    long count();
    Node add(MBR bounds, long id);
    long nodeId();
    List<Long> overlapping(MBR bounds);
    double area();
  }

  public final class Child implements RamaSerializable {
    public final MBR bounds;
    public final long id;

    public Child(MBR bounds, long id) {
      this.bounds = bounds;
      this.id = id;
    }
  }

  /** A node in the R-Tree */
  private class NonLeafNode implements Node {
    long id;
    long parent;
    List<Child> children;

    public NonLeafNode(long id, long parent) {
      this.id = id;
      this.parent = parent;
      this.children = new ArrayList<>();
    }

    public boolean isLeaf() {
      return false;
    }

    public long count() {
      return children.size();
    }

    public NonLeafNode add(MBR bounds, long id) {
      children.add(new Child(bounds, id));
      return this;
    }

    public long nodeId() {
      return id;
    }

    public List<Long> overlapping(MBR bounds) {
      return children.stream()
	.filter(child -> child.bounds.isIntersects(bounds))
	.map(child -> child.id)
	.collect(Collectors.toList());
    }

    public MBR bounds() {
      MBR bounds = children.get(0).bounds;
      for (Child child : children) {
	bounds = bounds.union(child.bounds);
      }
      return bounds;
    }

  }

  private class LeafNode implements Node {
    long id;
    long parent;
    List<Child> objects;

    public LeafNode(long id, long parent) {
      this.id = id;
      this.parent = parent;
      this.objects = new ArrayList<>();
    }

    public LeafNode(long id, long parent, Child child) {
      this.id = id;
      this.parent = parent;
      this.objects = new ArrayList<>(Arrays.asList(child));
    }

    public boolean isLeaf() {
      return true;
    }

    public long count() {
      return objects.size();
    }

    public LeafNode add(MBR bounds, long id) {
      objects.add(new Child(bounds, id));
      return this;
    }

    public long nodeId() {
      return id;
    }

    public List<Long> overlapping(MBR bounds) {
      return objects.stream()
	.filter(child -> child.bounds.isIntersects(bounds))
	.map(child -> child.id)
	.collect(Collectors.toList());
    }

    public MBR unionBounds() {
       MBR unionBounds = objects.get(0).bounds;
       for (Child child : objects) {
	 unionBounds = unionBounds.union(child.bounds);
       }
       return unionBounds;
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
      Child first = objects.get(0);
      List<Child> lowers = new ArrayList<>(Collections.nCopies(dimensions, first));
      List<Child> uppers = new ArrayList<>(Collections.nCopies(dimensions, first));
      final MBR unionBounds = unionBounds();
      for (Child child : objects) {
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

    public List<LeafNode> splitNodeImpl(long newNodeId) {
    // S1. [Pick first entry for each group.]
    // Apply Algorithm PickSeeds to
    // choose two entries to be the first elements of the groups. Assign each to
    // a group.
      List<Child> seeds = extremes();
      List<LeafNode> nodes
	= Arrays.asList(new LeafNode(id, parent, seeds.get(0)),
			new LeafNode(newNodeId, parent, seeds.get(1)));


      List<Child> toInsert = objects;
      toInsert.removeAll(seeds);

      // S2. [Check if done.] If all entries have been assigned, stop.
      while (!toInsert.isEmpty()) {
	// If one group has so few entries that all the rest must be assigned to
	// it in order for it to have the minimum number m, assign them and
	// stop.
	int nRemaining = toInsert.size();
	if (nRemaining <= m - nodes.get(0).count()) {
	  nodes.get(0).objects.addAll(toInsert);
	  break;
	} else if (nRemaining <= m - nodes.get(1).count()) {
	  nodes.get(1).objects.addAll(toInsert);
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
	  LeafNode candidate = nodes.get(i);
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
      return nodes;
    }

    public MBR bounds() {
      MBR bounds = objects.get(0).bounds;
      for (Child child : objects) {
	bounds = bounds.union(child.bounds);
      }
      return bounds;
    }

  }

  public RTree(final int dimensions,
	       final int M,
	       final int m,
	       final String pstatePrefix) {
    this.pstatePrefix = pstatePrefix;
    this.dimensions = dimensions;
    this.M = M;
    this.m = m;
    this.nodeId = new ModuleUniqueIdPState(pstatePrefix + "__nodeId");
    this.objectId = new ModuleUniqueIdPState(pstatePrefix + "__objectId");
    this.rootPstate = pstatePrefix + "__root";
    this.nodesPstate = pstatePrefix + "__nodes";
    this.objectsPstate = pstatePrefix + "__objects";
  }

  public <Top> void declarePStates(final ETLTopologyBase<Top> topology) {
    nodeId.declarePState(topology);
    objectId.declarePState(topology);
    topology.pstate(pstatePrefix,
		    PState.mapSchema(MBR.class, Object.class));
    topology.pstate(objectsPstate,
		    PState.mapSchema(Long.class, Object.class));
    topology.pstate(rootPstate, Object.class)
      .global()
      .initialValue(new LeafNode(-1,-1));

    topology.pstate(nodesPstate,
		    PState.mapSchema(Long.class, Node.class));
  }

  boolean isFull(Node node) {
    return node.count() >= M;
  }

  /** Select a leaf node in which to place a new index entry E */
  private Block chooseLeaf(final String boundsVar,
			   final String outVar,
			   final String leafNodeIsRootVar) {
    final String rootNodeVar = Helpers.genVar("rootNode");
    final String isLeafVar = Helpers.genVar("isLeaf");
    return Block
      // [Initialize.] Set N to be the root node.
      .localSelect(rootPstate, Path.stay()).out(rootNodeVar)
      // CL2. [Leaf check.] If N is a leaf, return N
      .each(Node::isLeaf, rootNodeVar).out(isLeafVar)
      .ifTrue(new Expr(Ops.EQUAL, isLeafVar, true),
	      Block
	      .each(Ops.IDENTITY, true).out(leafNodeIsRootVar)
	      .each(Ops.IDENTITY, rootNodeVar).out(outVar),

    // CL3. [Choose subtree.] If Af is not a leaf, let F be the entry in N whose
    // rectangle F.I needs least enlargement to include E.I. Resolve ties by
    // choosing the entry with the rectangle of smallest area
	      Block
	      .each(Ops.IDENTITY, false).out(leafNodeIsRootVar)
	      // TODO
	      .each(Ops.IDENTITY, null).out(outVar));
    // CL4. [Descend until a leaf is reached.] Set N to be the child node
    // pointed to by F.p and repeat from CL2.
  }

  /**
     Algorithm LinearPickSeeds. Select two entries to be the first elements of
     the groups.
  */
  private Block pickSeeds(final String leafNodeVar,
			  final String seedsVar) {
    // LPSl.[Find extreme rectangles along all dimensions.]

    // Along each dimension, find the entry whose rectangle has the highest low
    // side, and the one with the lowest high side. Record the separation.

    // LPS2. [Adjust for shape of the rectangle cluster.] Normalize the
    // separations by dividing by the width of the entire set along the
    // corresponding dimension.

    // LPS3. [Select the most extreme pair.] Choose the pair with the greatest
    // normalised separation alobg any dimension.
    return Block.each(LeafNode::extremes, leafNodeVar).out(seedsVar);
  }

  /** Divide a set of M+1 index entries into two groups.

      A Linear-Cost Algorithm Algorithm. This algorithm is linear in M and in
      the number of dimensions.
   */
  private Block splitNode(final String leafNodeVar,
			  final String leafNodeIsRootVar,
			  final String newNodesVar) {
    final String newNodeIdVar = Helpers.genVar("newNodeId");
    return Block
      .macro(objectId.genId(newNodeIdVar))
      .each(LeafNode::splitNodeImpl, leafNodeVar, newNodeIdVar).out(newNodesVar);
  }

  private Block adjustTree(final String newNodesVar) {
    // AT1. [Initialize.] Set N=L. If L was split previously, set NN to be the
    // resulting second node.

    // AT2. [Check if done.] If N is the root, stop.

    // AT3. [Adjust covering rectangle in parent entry.] Let P be the parent
    // node of N, and let EN be N's entry in P.  Adjust En.I so that it tightly
    // encloses all entry rectangles in N.

    // AT4. [Propagate node split upward.] If N has a partner NN resulting from
    // an earlier split, create a new entry ENN with ENN.p pointing to NN and Em
    // .I enclosing all rectangles in NN. Add Enn to P if there is room
    // Otherwise, invoke SplitNode to produce P and PP containing Em and all P’s
    // old entries.

    // AT5. [Move up to next level.] Set N=P and set NN-PP if a split
    // occurred. Repeat from AT2.
    return null;
  }

  /** Insert a new index entry E */
  private Block insert(final String boundsVar, final String objectVar) {
    final String leafNodeVar = Helpers.genVar("leafNode");
    final String leafNodeIsRootVar = Helpers.genVar("leafNodeIsRoot");
    final String isFullVar = Helpers.genVar("isFull");
    final String nodeIdVar = Helpers.genVar("nodeId");
    final String objectIdVar = Helpers.genVar("objectId");
    final String newNodeVar = Helpers.genVar("newNode");
    final String newNodesVar = Helpers.genVar("newNodes");
    final String newNodeIdVar = Helpers.genVar("newNodeId");

    return
      // 11. [Find position for new record.]
      // Invoke ChooseLeaf to select a leaf node L in which to place E.
      Block
      .macro(chooseLeaf(boundsVar, leafNodeVar, leafNodeIsRootVar))

      // 12. [Add record to leaf node.]
      .each(RTree<T>::isFull, this, leafNodeVar).out(isFullVar)
      // If L doesn't has room for another entry
      .ifTrue(isFullVar,
	      // invoke splitNode to obtain L and LL containing E and all the
	      // old entries of L.
	      Block
	      .macro(splitNode(leafNodeVar, leafNodeIsRootVar, newNodesVar))
	      .each(Ops.EXPLODE, newNodesVar).out(newNodeVar)
	      .each(Node::nodeId, newNodeVar).out(newNodeIdVar)
	      .localTransform(nodesPstate, Path.key(newNodeIdVar).termVal(newNodeVar))
	      ,
	      // install E in L
	      Block
	      .macro(nodeId.genId(objectIdVar))
	      .each(Node::nodeId, leafNodeVar).out(nodeIdVar)
	      .each(Node::add, leafNodeVar, boundsVar, objectIdVar).out(leafNodeVar)
	      .each(Ops.IDENTITY, null).out(newNodeVar)
	      .localTransform(objectsPstate, Path.key(objectIdVar).termVal(objectVar))
	      .ifTrue(leafNodeIsRootVar,
		      Block.localTransform(rootPstate, Path.termVal(leafNodeVar)),
		      Block.localTransform(nodesPstate,
					   Path.key(nodeIdVar).termVal(leafNodeVar)))
	      .each(Ops.TUPLE, leafNodeVar).out(newNodesVar))

      // 13. [Propagate changes upward.] Invoke AdjustTree on L, also passing LL
      // if a split was performed.
      .macro(adjustTree(newNodesVar))

    // 14. [Grow tree taller.] If node split propagation caused the root to
    // split, create a new root whose children are the two resulting nodes.
      ;
  }

  /**
     Given a leaf node L from which an entry has been deleted, eliminate the
     node if it has too few entries and relocate its entries. Propagate node
     elimination upward as necessary. Adjust all covering rectangles on the path
     to the root, making them smaller if possible.
   */
  private Block condenseTree() {
    // CT1. [Initialize.] Set N=L. Set Q, the-set of eliminated nodes, to be
    // empty.

    // CT2. [Find parent entry.] If N is the root, go to CT6. Otherwise let P be
    // the parent of N, and let EN be N's entry in P.

    // CT3. [Eliminate under-full node.] If N has fewer than m entries, delete
    // EN from P and add N to set Q.

    // CT4. [Adjust covering rectangle.] -If N has not been eliminated, adjust
    // EN.I to tightly contain all entries in N.

    // CT5. [Move up one level in tree.] Set N=P and repeat from CT2.

    // CT6. [Re-insert orphaned entries] Re-insert all entries of nodes in set
    // Q. Entries from the eliminated lead nodes are re-inserted in tree leaves
    // as described by the Insert algorithm, but entries from higher level nodes
    // must be placed higher in the tree, so that leaves of their dependent
    // sub-trees will be on the same level as leaves of the main tree.
    return null;
  }

  /** Given an R-tree whose root node is T, find the leaf node containing the
   * index entry E */
  private Block findLeaf() {
    // FL1. [Search subtrees.] If T is not a leaf, check each entry F in T to
    // determine if F.I overlaps E.I. For each such entry inyoke FindLeaf on the
    // tree whose root is pointed to by F.p until E is found or all entries have
    // been checked.

    // FL2. [Search leaf node for record.] If T is a leaf, check each entry to
    // see if it matches E.  If E is found, return T.
    return null;
  }

  private Block delete() {
    // D1. [Find node containing record.] Invoke FindLeaf to locate the leaf
    //     node L containing E. Stop if the record was not found.
    // D2. [Delete record.] Remove E from L.
    // D3. [Propagate changes.] Invoke Con- denseTree, passing L.
    // D4. [Shorten tree.] If the root node hasn only one child after the tree
    //     has been adjusted, make thé child the new root.
    return null;
  }

  /** Given an R-tree whose root node is T, find all index records whose
      rectangles overlap a search rectangle S.
  */
  private Block search(final String boundsVar, final String rootVar, final String outVar) {
    final String isLeafVar = Helpers.genVar("isLeaf");
    final String objectIdsVar = Helpers.genVar("objectIds");
    final String objectIdVar = Helpers.genVar("objectId");
    return Block
      .each(Node::isLeaf, rootVar).out(isLeafVar)
      .ifTrue(new Expr(Ops.EQUAL, isLeafVar, false),
      // S1. [Search subtrees.] If T is not a leaf, check each entry E to
      // determine whether E.I overlaps S. For all overlapping entries, invoke
      // Search on the tree whose root node is pointed to by E.p .
	      Block.each(Ops.IDENTITY, "todo").out(outVar),
      // S2. [Search leaf node.] If T is a leaf, check all entries E to
      // determine whether E.I overlaps S. If so, E is a qualifying record.
	      Block
	      .each(Node::overlapping, rootVar, boundsVar).out(objectIdsVar)
	      .each(Ops.EXPLODE, objectIdsVar).out(objectIdVar)
	      .localSelect(objectsPstate, Path.key(objectIdVar)).out(outVar)
      );
  }

  public Block addObject(final String boundsVar, final String objectVar) {
    return Block.macro(insert(boundsVar, objectVar));
  }


  public void declareQueries(final Topologies topologies) {
    topologies.query("objectsInBounds", "*bounds").out("*objects")
      .localSelect(rootPstate, Path.stay()).out("*root")
      .macro(search("*bounds", "*root", "*objects"))
      .originPartition()
      .agg(Agg.list("*objects")).out("*objects");
  }
}
