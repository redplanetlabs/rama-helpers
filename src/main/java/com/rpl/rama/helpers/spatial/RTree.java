package com.rpl.rama.helpers.spatial;

import clojure.lang.IPersistentVector;
import clojure.lang.PersistentVector;
import clojure.lang.RT;
import clojure.lang.Var;
import clojure.java.api.Clojure;
import clojure.lang.APersistentVector;

import com.rpl.rama.Agg;
import com.rpl.rama.Block;
import com.rpl.rama.CompoundAgg;
import com.rpl.rama.Expr;
import com.rpl.rama.Helpers;
import com.rpl.rama.LoopVars;
import com.rpl.rama.PState;
import com.rpl.rama.Path;
import com.rpl.rama.RamaSerializable;
import com.rpl.rama.RamaModule.Topologies;
import com.rpl.rama.helpers.ModuleUniqueIdPState;
import com.rpl.rama.helpers.RamaAssert;
import com.rpl.rama.module.MicrobatchTopology;
import com.rpl.rama.ops.Ops;
import com.rpl.rama.ops.OutputCollector;
import com.rpl.rama.ops.RamaFunction1;
import com.rpl.rama.ops.RamaFunction2;
import com.rpl.rama.ops.RamaFunction3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
// import org.apache.logging.log4j.LogManager;
// import org.apache.logging.log4j.Logger;

import static com.rpl.rama.helpers.TopologyUtils.extractJavaFields;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import rpl.rama.java_api.integration__init;

public class RTree implements RamaSerializable {
  private final int dimensions;
  private final ModuleUniqueIdPState idGenerator;
  private final String nodesPstate;
  private final String rootPstate;

  private final int M;
  private final int m;

  public static class AddObject implements RamaSerializable {
    public final MBR bounds;
    public final long objectId;

    public AddObject(final MBR bounds, long objectId) {
      this.bounds = bounds;
      this.objectId = objectId;
    }
  }

  // Create a logger instance
  private static final Logger LOGGER = LoggerFactory.getLogger(RTree.class);
  // private static final Logger LOGGER = LogManager.getLogger(RTree.class);

  public RTree(final int dimensions,
               final int M,
               final int m,
               final String treeName) {
    this.dimensions = dimensions;
    this.M = M;
    this.m = m;
    this.idGenerator = new ModuleUniqueIdPState("$$" + treeName + "__nodeId");
    this.rootPstate = "$$" + treeName + "__root";
    this.nodesPstate = "$$" + treeName + "__nodes";
  }

  private void declarePStates(final MicrobatchTopology topology) {
    idGenerator.declarePState(topology);
    topology.pstate(rootPstate, Object.class);
    topology.pstate(nodesPstate,
                    PState.mapSchema(Long.class, Node.class));
  }

  public NonLeafNode createNonLeafNode(long id, Node child1, Node child2) {
    return (NonLeafNode) new NonLeafNode(id, id)
        .add(child1.bounds(), child1.nodeId())
        .add(child2.bounds(), child2.nodeId());
    }

  public static NonLeafNode createRootNode(long id) {
    return new NonLeafNode(id, id);
    }

  boolean isFull(Node node) {
    return node.count() >= M;
  }

  /** Select a leaf node in which to place a new index entry E */
  private Block chooseLeaf(final String rootNodeVar,
                           final String boundsVar,
                           final String leafNodeVar,
                           final String leafNodeIsRootVar) {
    final String isLeafVar = Helpers.genVar("isLeaf");
    return Block
        // [Initialize.] Set N to be the root node.
        // CL2. [Leaf check.] If N is a leaf, return N
        .each(Node::isLeaf, rootNodeVar).out(isLeafVar)
        .each(Ops.LOG_DEBUG,
              LOGGER,
              new Expr(Ops.TO_STRING, "chooseLeaf root isLeaf: " , isLeafVar))
        .ifTrue(new Expr(Ops.EQUAL, isLeafVar, true),
                Block
                .each(Ops.IDENTITY, true).out(leafNodeIsRootVar)
                .each(Ops.IDENTITY, rootNodeVar).out(leafNodeVar),

                // CL3. [Choose subtree.] If Af is not a leaf, let F be the entry in N whose
                // rectangle F.I needs least enlargement to include E.I. Resolve ties by
                // choosing the entry with the rectangle of smallest area
                Block
                .loopWithVars(
                  LoopVars.var("*node", rootNodeVar),
                  // CL4. [Descend until a leaf is reached.] Set N to be the child node
                  // pointed to by F.p and repeat from CL2.
                  Block
                  .yieldIfOvertime()
                  .each(Node::isLeaf, "*node").out(isLeafVar)
                  .each(Node::nodeId, "*node").out("*nodeId")
                  .each(Ops.LOG_DEBUG,
                        LOGGER,
                        new Expr(Ops.TO_STRING,
                                 "chooseLeaf loop, node: ",
                                 "*node"))
                  .ifTrue(
                    new Expr(Ops.EQUAL, isLeafVar, true),
                    // We have reached a leaf node, so emit it
                    Block.emitLoop("*node"),
                    // Not at a leaf node yet.
                    Block
                    .each(Ops.IDENTITY, false).out(leafNodeIsRootVar)
                    .each(Node::chooseChild, "*node", boundsVar).out("*childId")
                    .macro(readNode("*childId", "*childNode"))
                    .each(Ops.LOG_DEBUG,
                          LOGGER,
                          new Expr(Ops.TO_STRING,
                                   "Chosen child: ", "*childNode"))
                    .continueLoop("*childNode")))
                .out(leafNodeVar))
        .each(Ops.LOG_DEBUG,
              LOGGER,
              new Expr(Ops.TO_STRING, "Chosen leaf: ", leafNodeVar))
        .macro(RamaAssert.assertMacro(Node::isLeaf, leafNodeVar));
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
  private Block splitNode(final String leafNodeVar, final String newNodeVar) {
    final String newNodeIdVar = Helpers.genVar("newNodeId");
    return Block
      .macro(idGenerator.genId(newNodeIdVar))
      .each(LeafNode::splitNode, leafNodeVar, newNodeIdVar, m).out(newNodeVar);
  }

  // /** Adjust tree.
  //  * Persists newNodes into the tree.
  //  */
  // private Block adjustTree(final String nodeVar,
  //                          final String newNodeVar,
  //                          /* final String nodeIsRootVar, */
  //                          /* final String needsNewRootVar, */
  //                          final String newParentVar) {
  //   final String parentIdVar = Helpers.genVar("parentId");
  //   final String parentVar = Helpers.genVar("parent"); // P
  //   final String nodeIdVar = Helpers.genVar("nodeId");
  //   final String isParentFullVar = Helpers.genVar("isParentFull");
  //   final String newParentsVar = Helpers.genVar("newParents");
  //   final String newNodeBoundsVar = Helpers.genVar("newNodeBounds");
  //   final String newNodeIdVar = Helpers.genVar("newNodeIdVar");
  //   final String newParentIdVar = Helpers.genVar("newParentId");
  //   final String nodeIsRootVar = Helpers.genVar("nodeIsRoot");
  //   // AT1. [Initialize.] Set N=L. If L was split previously, set NN to be the
  //   // resulting second node.
  //   return Block
  //     .each(Ops.PRINTLN, "adjustTree")
  //     // .each(Ops.LOG_DEBUG, logger, "adjustTree")
  //     /* .each(Ops.EXPAND, newNodesVar).out(nodeVar, newNodeVar) */
  //     .each(Node::nodeId, nodeVar).out(nodeIdVar)
  //     // AT2. [Check if done.] If N is the root, stop.
  //     // .loopWithVars(LoopVars
  //     // 		    .var(nodeIdVar, nodeIdVar)
  //     // 		    .var(nodeVar, nodeVar)
  //     // 		    .var(newNodeVar, newNodeVar),
  //     // 		    Block
  //     // 	    .ifTrue(new Expr(Ops.EQUAL, nodeIsRootVar, true),
  //     // 		    Block
  //     // 		    .each(Ops.IDENTITY, nodeVar).out(outVar)
  //     // 		    .emitLoop(outVar),

  //     // Block

  //     // AT3. [Adjust covering rectangle in parent entry.]
  //     .each(Node::isRoot, nodeVar).out(nodeIsRootVar)
  //     // .each(Ops.LOG_DEBUG, logger, "adjustTree newNode", newNodeVar)
  //     .each(Ops.PRINTLN, "adjustTree newNode", newNodeVar)
  //     .ifTrue(new Expr(Ops.AND,
  //                      new Expr(Ops.IS_NOT_NULL, newNodeVar),
  //                      new Expr(Ops.EQUAL, nodeIsRootVar, true)),
  //             Block
  //             .macro(idGenerator.genId(newParentIdVar))
  //             .each(RTree::createNonLeafNode,
  //                   this,
  //                   newParentIdVar,
  //                   nodeVar,
  //                   newNodeVar).out(newParentVar),

  //             // Let P be the parent node of N
  //             Block
  //             .each(Node::parentId, nodeVar).out(parentIdVar)
  //             // .select(objectsPstate, Path.key(parentIdVar)).out(parentVar)
  //             // .each(RTree<T>::isFull, this, parentVar).out(isParentFullVar)

  //             // and let EN be N's entry in
  //             // P.  Adjust En.I so that it tightly encloses all entry
  //             // rectangles in N.
  //             .each(Node::updateChild, parentVar, nodeVar)

  //             // AT4. [Propagate node split upward.]
  //             // If N has a partner NN resulting from an earlier split,
  //             // create a new entry ENN with ENN.p pointing to NN and Em
  //             // .I enclosing all rectangles in NN. Add Enn to P if there
  //             // is room Otherwise, invoke SplitNode to produce P and PP
  //             // containing Em and all P’s old entries.
  //             // .each(Ops.IDENTITY, null).out(newNodeVar)
  //             .ifTrue(new Expr(Ops.IS_NOT_NULL, newNodeVar),
  //                     Block
  //                     .each(Node::bounds, newNodeVar).out(newNodeBoundsVar)
  //                     .each(Node::nodeId, newNodeVar).out(newNodeIdVar)
  //                     .macro(insertInNode(newNodeVar, newNodeBoundsVar, newNodeIdVar, newParentVar))
  //                     // Block
  //                     // .each(RTree<T>::isFull, this, parentVar).out(isParentFullVar)
  //                     // .ifTrue(new Expr(Ops.EQUAL, isParentFullVar, true),
  //                     // 	    Block
  //                     // 	    .macro(splitNode(parentVar, isParentFullVar, newParentsVar))
  //                     // 	    .each(Ops.EXPAND, newParentsVar).out(parentVar, newNodeVar)
  //                     // 	    .each(Node::nodeId, parentVar).out(parentIdVar))
  //                     )
  //             // AT5. [Move up to next level.] Set N=P and set NN-PP if a
  //             // split occurred. Repeat from AT2.
  //             // .ifTrue(new Expr(Ops.EQUAL, parentIdVar, nodeIdVar),
  //             // 	    Block.emitLoop(),
  //             // 	    Block.continueLoop(parentIdVar, parentVar, newNodeVar))))
  //             );
  // }

  // /** Insert entry or child into node.
  //  */
  // private Block insertInNode(final String nodeVar,
  //                            final String boundsVar,
  //                            /* This can be object or child node id */
  //                            final String idVar,
  //                            /* this is the output, a new sibling node */
  //                            final String newSiblingNodeVar) {
  //   final String leafNodeIsRootVar = Helpers.genVar("leafNodeIsRoot");
  //   final String isFullVar = Helpers.genVar("isFull");
  //   final String nodeIdVar = Helpers.genVar("nodeId");

  //   return Block
  //     .each(Ops.PRINTLN, "insertInNode", nodeVar, boundsVar, idVar)

  //     // 12. [Add record to node.]
  //     .each(RTree::isFull, this, nodeVar).out(isFullVar)
  //     .each(Ops.PRINTLN, "insertInNode isFull", isFullVar)

  //     // If L doesn't has room for another entry
  //     .ifTrue(isFullVar,
  //             // invoke splitNode to obtain L and LL containing E and all the
  //             // old entries of L.
  //             Block
  //             .macro(splitNode(nodeVar, newSiblingNodeVar))
  //             ,
  //             // install E in L
  //             Block
  //             .each(Ops.IDENTITY, Arrays.asList()).out(newSiblingNodeVar))

  //     .each(Node::nodeId, nodeVar).out(nodeIdVar)
  //     // split could go async, so may need batch blocks and a depot for updates
  //     .each(Node::add, nodeVar, boundsVar, idVar)
  //     .each(Ops.PRINTLN, "insertInNode after add", nodeVar)
  //     .each(Node::isRoot, nodeVar).out(leafNodeIsRootVar)
  //     .ifTrue(leafNodeIsRootVar,
  //             Block.localTransform(rootPstate, Path.termVal(nodeVar)),
  //             Block.localTransform(nodesPstate,
  //                                  Path.key(nodeIdVar).termVal(nodeVar)))
  //     .each(Ops.PRINTLN, "insertInNode done");
  // }

  // /** Perform an insert, creating a new node if needed and updating the parent.
  //     If new node is created, insert the created node into the parent and
  //     repeat.
  //  */
  // private Block insertLoop(final String nodeVar,
  //                          final String childBoundsVar,
  //                          /* This can be object or child node id */
  //                          final String childIdVar,
  //                          /* this is the output, the new parent node */
  //                          final String parentNodeVar) {
  //   final String newSiblingVar = Helpers.genVar("newSibling");
  //   final String newSiblingBoundsVar = Helpers.genVar("newSiblingBounds");
  //   final String newSiblingIdVar = Helpers.genVar("newSiblingId");

  //   return Block
  //     .loopWithVars(LoopVars
  //                   .var(nodeVar, nodeVar) // node to insert into
  //                   .var(childBoundsVar, childBoundsVar)
  //                   .var(childIdVar, childIdVar),
  //                   Block
  //                   .macro(insertInNode(nodeVar,
  //                                       childBoundsVar,
  //                                       childIdVar,
  //                                       newSiblingVar))
  //                   .each(Ops.PRINTLN,
  //                         "insertLoop, newSiblingVar",
  //                         newSiblingVar)

  //                   // 13. [Propagate changes upward.] Invoke AdjustTree on L,
  //                   // also passing LL if a split was performed.
  //                   .macro(adjustTree(nodeVar, newSiblingVar, parentNodeVar))

  //                   .ifTrue(new Expr(Ops.IS_NULL, newSiblingVar),
  //                           Block.emitLoop(nodeVar),
  //                           Block
  //                           .each(Node::nodeId, newSiblingVar).out(newSiblingIdVar)
  //                           .each(Node::bounds, newSiblingVar).out(newSiblingBoundsVar)
  //                           .continueLoop(parentNodeVar, newSiblingBoundsVar, newSiblingIdVar)))
  //     ;
  // }


  /** Write the root node value to all partitions */
  private Block broadcastRootNodeValue(final String rootNodeVar) {
    return Block
        .batchBlock(
          Block
          .allPartition()
	  .macro(writeRoot(rootNodeVar)));
  }

  private LeafNode constructRoot(long id) {
      return new LeafNode(id, id);
  }

  /** Set rootNodeVar to be the local copy of the root node.
      If the root node does not exist it is created.
   */
  private Block rootNode(final String rootNodeVar) {
    final String currentRootNodeVar = Helpers.genVar("rootNode");
    final String rootNodeIdVar = Helpers.genVar("rootNodeId");
    return Block
      .each(Ops.PRINTLN, "rootNode")
      .localSelect(rootPstate, Path.stay()).out(currentRootNodeVar)
      .ifTrue(new Expr(Ops.IS_NULL, currentRootNodeVar),
              Block
              .each(Ops.PRINTLN, "Creating root node")
              .macro(idGenerator.genId(rootNodeIdVar))
              .each(RTree::constructRoot, this, rootNodeIdVar).out(rootNodeVar)
              .macro(broadcastRootNodeValue(rootNodeVar)),
              Block
              .each(Ops.PRINTLN, "Root node already exists")
              .each(Ops.IDENTITY, currentRootNodeVar).out(rootNodeVar))
      .each(Ops.PRINTLN, "Root node", rootNodeVar);
  }

  // /** Insert a new index entry E */
  // private Block insert(final String boundsVar, final String objectVar) {
  //   final String leafNodeVar = Helpers.genVar("leafNode");
  //   final String leafNodeIsRootVar = Helpers.genVar("leafNodeIsRoot");
  //   final String idVar = Helpers.genVar("id");
  //   final String rootNodeVar = Helpers.genVar("rootNode");
  //   final String rootNodeIdVar = Helpers.genVar("rootNodeId");
  //   final String newRootNodeVar = Helpers.genVar("newRootNode");
  //   final String newRootNodeIdVar = Helpers.genVar("newRootNodeId");

  //   return
  //     // 11. [Find position for new record.]
  //     // Invoke ChooseLeaf to select a leaf node L in which to place E.
  //     Block
  //     // [Initialize.] Set N to be the root node.
  //     .macro(rootNode(rootNodeVar))
  //     .each(Node::nodeId, rootNodeVar).out(rootNodeIdVar)
  //     .macro(chooseLeaf(rootNodeVar, leafNodeVar, leafNodeIsRootVar))
  //     .macro(idGenerator.genId(idVar))
  //     // .localTransform(objectsPstate, Path.key(idVar).termVal(objectVar))
  //     .macro(insertInNode(leafNodeVar, boundsVar, idVar, newRootNodeVar))
  //     // // 12. [Add record to leaf node.]
  //     // .each(RTree<T>::isFull, this, leafNodeVar).out(isFullVar)
  //     // // If L doesn't has room for another entry
  //     // .ifTrue(isFullVar,
  //     // 	      // invoke splitNode to obtain L and LL containing E and all the
  //     // 	      // old entries of L.
  //     // 	      Block
  //     // 	      .macro(splitNode(leafNodeVar, leafNodeIsRootVar, newNodesVar))
  //     // 	      .each(Ops.EXPLODE, newNodesVar).out(newNodeVar)
  //     // 	      .each(Node::nodeId, newNodeVar).out(newNodeIdVar)
  //     // 	      .localTransform(nodesPstate, Path.key(newNodeIdVar).termVal(newNodeVar))
  //     // 	      ,
  //     // 	      // install E in L
  //     // 	      Block
  //     // 	      .macro(nodeId.genId(idGeneratorVar))
  //     // 	      .each(Node::nodeId, leafNodeVar).out(nodeIdVar)
  //     // 	      .each(Node::add, leafNodeVar, boundsVar, idGeneratorVar).out(leafNodeVar)
  //     // 	      .each(Ops.IDENTITY, null).out(newNodeVar)
  //     // 	      .localTransform(objectsPstate, Path.key(idGeneratorVar).termVal(objectVar))
  //     // 	      .ifTrue(leafNodeIsRootVar,
  //     // 		      Block.localTransform(rootPstate, Path.termVal(leafNodeVar)),
  //     // 		      Block.localTransform(nodesPstate,
  //     // 					   Path.key(nodeIdVar).termVal(leafNodeVar)))
  //     // 	      .each(Ops.TUPLE, leafNodeVar).out(newNodesVar))

  //     // // 13. [Propagate changes upward.] Invoke AdjustTree on L, also passing LL
  //     // // if a split was performed.
  //     // .macro(adjustTree(newNodesVar, nodeIsRootVar, needsNewRootVar, rootNodeVar))

  //   // 14. [Grow tree taller.] If node split propagation caused the root to
  //   // split, create a new root whose children are the two resulting nodes.
  //     .each(Node::nodeId, newRootNodeVar).out(newRootNodeIdVar)
  //     .ifTrue(new Expr(Ops.NOT_EQUAL, rootNodeIdVar, newRootNodeIdVar),
  //             Block
  //             .localTransform(rootPstate, Path.stay().termVal(newRootNodeVar)));
  // }

  protected Block readNode(final String nodeIdVar, final String nodeVar) {
    return Block
        .each(Ops.LOG_DEBUG, LOGGER,
              new Expr(Ops.TO_STRING, "readNode: ", nodeIdVar))
        .hashPartition(nodeIdVar)
        .localSelect(nodesPstate, Path.key(nodeIdVar)).out(nodeVar);
  }

  protected Block writeNode(final String nodeIdVar, final String nodeVar) {
    return Block
        .each(Ops.LOG_DEBUG, LOGGER,
              new Expr(Ops.TO_STRING, "writeNode: ", nodeIdVar, " ", nodeVar))
        .hashPartition(nodeIdVar)
        .localTransform(nodesPstate, Path.key(nodeIdVar).termVal(nodeVar));
  }

  protected Block writeRoot(final String nodeVar) {
    return Block
      .each(Ops.LOG_DEBUG, LOGGER,
	    new Expr(Ops.TO_STRING, "writeRoot: ", nodeVar))
      .localTransform(rootPstate, Path.termVal(nodeVar));
  }

  /** Perform all operations from nodeOpsVar on nodeVar.

      This does not persist the updated node, or the new siblings.

      newSiblingIdVar will contain any new sibling nodes that have been created.
   */

  protected static Block updateNode(
    final int branchingFactor,
    final ModuleUniqueIdPState idGenerator,
    final String nodeVar,
    final String nodeOpsVar,
    // outputs
    final String newSiblingsVar) {

    // this could just be a java function, except for the id-gen
    return Block
        .each(Ops.LOG_DEBUG,
              LOGGER,
              new Expr(Ops.TO_STRING, "updateNode ", nodeVar, " ", nodeOpsVar))
        // NOTE - this is a temp var to avoid an array list literal in the Loop
        // var, which causes it to use the object cache.
        .each(RTreeHelpers::newArrayList).out("*emptySiblings")
        .each(Ops.LOG_DEBUG, LOGGER, "before loop")
        .loopWithVars(
          LoopVars
          .var("*currentNode", nodeVar)
          .var("*nodeOpsRemaining", nodeOpsVar)
          .var("*siblings", "*emptySiblings"),
          Block
          .yieldIfOvertime()
          .each(Ops.LOG_DEBUG,
                LOGGER,
                new Expr(Ops.TO_STRING,
                         "start of loop, current node", "*currentNode"))
          .each(Ops.LOG_DEBUG,
                LOGGER,
                new Expr(Ops.TO_STRING,
                         "start of loop, opsRemaining", "*nodeOpsRemaining"))
          .ifTrue(
            new Expr(Ops.AND,
                     new Expr(Node::isFull, "*currentNode", branchingFactor),
                     new Expr(Ops.NOT,
                              new Expr(APersistentVector::isEmpty,
                                       "*nodeOpsRemaining"))),
            Block
            .each(Ops.LOG_DEBUG, LOGGER, "Needs split block")
            .macro(idGenerator.genId("*newNodeId"))
            .each(Node::newSibling,
                  "*currentNode",
                  "*newNodeId").out("*newNode")
            .each((final List<Node> l, final Node node) -> {
                l.add(node);
                return l;
            },
              "*siblings",
              "*newNode")
            .continueLoop("*newNode",
                          "*nodeOpsRemaining",
                          "*siblings")
            ,
            Block
            .each(Ops.LOG_DEBUG, LOGGER, "no split block")
            .each(List<Node>::size, "*nodeOpsRemaining").out("*opCount")
            .ifTrue(new Expr(Ops.EQUAL, "*opCount", 0),
                    Block
                    .each(Ops.LOG_DEBUG, LOGGER, "Emitting")
                    .emitLoop("*siblings"),
                    Block
                    .each(Ops.LOG_DEBUG, LOGGER, "more ops")
                    .each(APersistentVector::peek,
                          "*nodeOpsRemaining").out("*nodeOp")
                    .macro(extractJavaFields("*nodeOp", "*bounds", "*objectId"))
                    .each(Node::add, "*currentNode", "*bounds", "*objectId")
                    .each(APersistentVector::pop,
                          "*nodeOpsRemaining").out("*nodeOpsRemaining")
                    .continueLoop("*currentNode",
                                  "*nodeOpsRemaining",
                                  "*siblings"))
                  )).out(newSiblingsVar)
        .each(Ops.LOG_DEBUG,
              LOGGER,
              new Expr(Ops.TO_STRING,
                       "after loop, new siblings", newSiblingsVar));
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
    final String idGeneratorsVar = Helpers.genVar("idGenerators");
    final String idGeneratorVar = Helpers.genVar("idGenerator");
    return Block
        .loopWithVars(
          LoopVars.var("*node", rootVar),
          Block
          .yieldIfOvertime()
          .each(Node::isLeaf, "*node").out(isLeafVar)
          .each(Ops.LOG_DEBUG,
                LOGGER,
                new Expr(Ops.TO_STRING, "search isLeaf: ", isLeafVar))
          .ifTrue(new Expr(Ops.EQUAL, isLeafVar, false),
                  // S1. [Search subtrees.] If T is not a leaf, check each entry E to
                  // determine whether E.I overlaps S. For all overlapping entries, invoke
                  // Search on the tree whose root node is pointed to by E.p .
                  Block
                  .each(Node::overlapping, "*node", boundsVar).out("*childIds")
                  .each(Ops.LOG_DEBUG,
                        LOGGER,
                        new Expr(Ops.TO_STRING,
				 "search child ids: ", "*childIds"))
                  .each(Ops.EXPLODE, "*childIds").out("*childId")
                  .macro(readNode("*childId", "*child"))
                  .each(Ops.LOG_DEBUG,
                        LOGGER,
                        new Expr(Ops.TO_STRING, "Child: ", "*child"))
                  .each(Ops.LOG_DEBUG, LOGGER, "emitting from inner loop")
                  .each(Ops.LOG_DEBUG, LOGGER,
                        new Expr(Ops.TO_STRING,
                                 "Continue outer loop: ", "*child"))
                  .continueLoop("*child"),

                  // S2. [Search leaf node.] If T is a leaf, check all entries E to
                  // determine whether E.I overlaps S. If so, E is a qualifying record.
                  Block
                  .each(Ops.PRINTLN, "Single leaf node")
                  .each(Node::overlapping, "*node", boundsVar).out("*ids")
                  .each(Ops.PRINTLN, "Matching objects", "*ids")
                  .each(Ops.EXPLODE, "*ids").out("*id")
                  .emitLoop("*id")))
        .out(outVar);
  }

  private static <T> List<T> emptyList() {
    return new ArrayList<T>();
  }

  private static PersistentVector emptyVec() {
    return PersistentVector.EMPTY;
  }

  private static Object firstList(List<Object> l) {
    return l.get(0);
  }

  private static <T> List<T> restList(List<T> l) {
    l.remove(0);
    return l;
  }

  // private static <T> List<T> conjList(List<T> l, T n) {
  //   LOGGER.debug("conjList", n.toString());
  //   l.add(n);
  //   return l;
  // }

  private static List<Object> conjList(List<Object> l, Object n) {
    LOGGER.debug("conjList", n.toString());
    l.add(n);
    return l;
  }

  private static <T> List<T> conjList1(List<T> l, T n) {
    LOGGER.debug("conjList", n.toString());
    l.add(n);
    return l;
  }

  private static boolean isEmptyList(List<Node> l) {
    return l.isEmpty();
  }

  private static List<Node> addAllList(List<Node> l1, List<Node> l2) {
    l1.addAll(l2);
    return l1;
  }

  private static Var into;

  {
    RT.init();

        // // Alternatively, you can require a namespace which will
        // // also initialize the runtime
        // IFn require = Clojure.var("clojure.core", "require");
        // require.invoke(Clojure.read("clojure.core"));

        // Now you can resolve vars as before
    into = (Var) Clojure.var("clojure.core", "into");
  }

  private static PersistentVector addAllVec(PersistentVector l1,
                                            List l2) {
    return (PersistentVector)into.invoke(l1, l2);
  }

  private Block verify(final String rootVar, final String outVar) {
    return Block
        .loopWithVars(
          LoopVars.var("*node", rootVar),
          Block
          .yieldIfOvertime()
          .ifTrue(new Expr(Node::isLeaf, "*node"),
                  Block.emitLoop(true),

                  Block
                  .each(Node::getChildren, "*node").out("*children")
                  .each(Node::unionBounds, "*node").out("*bounds")
                  .each(MBR::empty, dimensions).out("*emptyMBR")
                  .each(RTree::<Node>emptyList).out("*emptyChildrenNodes")
                  .loopWithVars(
                    LoopVars
                    .var("*children", "*children")
                    .var("*totalBounds", "*emptyMBR")
                    .var("*childrenNodes", "*emptyChildrenNodes"),
                    Block
                    .ifTrue(
                      new Expr(RTree::isEmptyList, "*children"),
                      Block.emitLoop("*totalBounds", "*childrenNodes"),
                      Block
                      .each(RTree::firstList, "*children").out("*child")
                      .macro(RamaAssert.assertMacro(Child::isChild, "*child"))
                      .macro(extractJavaFields("*child", "*bounds", "*id"))
                      .macro(readNode("*id", "*childNode"))
                      .each(Node::unionBounds, "*childNode").out("*childBounds")
                      .ifTrue(
                        new Expr(Ops.NOT_EQUAL, "*childBounds", "*bounds"),
                        Block
                        .each(
                          Ops.LOG_ERROR,
                          LOGGER,
                          new Expr(Ops.TO_STRING,
                                   "Child bounds did not match: Chiild: ",
                                   "*bounds",
                                   " node union bounds: ",
                                   "*childBounds")))
                      .continueLoop(
                        new Expr(RTree::<Node>restList, "*children"),
                        new Expr(MBR::union, "*totalBounds", "*bounds"),
                        new Expr(RTree::conjList,
                                 "*childrenNodes",
                                 "*childNode"))))
                  .out("*totalBounds", "*childrenNodes")
                  .ifTrue(
                    new Expr(Ops.NOT_EQUAL, "*totalBounds", "*bounds"),
                    Block
                    .each(
                      Ops.PRINTLN,
                      new Expr(Ops.TO_STRING,
                               "total bounds did not match: total: ",
                               "*totalBounds",
                               " bounds: ",
                               "*bounds"))
                    .each(
                      Ops.LOG_ERROR,
                      LOGGER,
                      new Expr(Ops.TO_STRING,
                               "total bounds did not match: total: ",
                               "*totalBounds",
                               " bounds: ",
                               "*bounds")))

                  .each(Ops.EXPLODE, "*childrenNodes").out("*childNode")
                  .continueLoop("*childNode")))
        .out(outVar);
  }

  private Block dump(final String rootVar) {
    return Block
        .each(Ops.LOG_INFO,
              LOGGER,
              new Expr(Ops.TO_STRING, "Root node: ", rootVar))
        .localSelect(nodesPstate, Path.all()).out("*node")
        .each(Ops.LOG_INFO,
              LOGGER,
              new Expr(Ops.TO_STRING, "Node: ", "*node"));
  }


  private Block dumpDot(final String elementsVar) {
    return Block
      .allPartition()
      .each(Ops.LOG_DEBUG,
	    LOGGER,
	    new Expr(Ops.TO_STRING,
		     "freshBatchSource task-id: ",
		     new Expr(Ops.CURRENT_TASK_ID)))

      .localSelect(nodesPstate,
		   Path
		   .all()
		   .last()
		   .multiPath(Path.view(Node::dotNodes).all(),
			      Path.view(Node::dotEdges).all())
		   ).out("*elem")
      .each(Ops.LOG_DEBUG,
	    LOGGER,
	    new Expr(Ops.TO_STRING, "other element: ", "*elem"))
      .anchor("otherNodes")

      .freshBatchSource()
      .localSelect(rootPstate, Path.stay()).out("*root")
      .each(Ops.LOG_DEBUG,
	    LOGGER,
	    new Expr(Ops.TO_STRING,
		     "task-id: ",
		     new Expr(Ops.CURRENT_TASK_ID)))
      .select("*root",
	      Path.multiPath(Path.view(Node::dotNodes).all(),
			     Path.view(Node::dotEdges).all())
	      ).out("*elem")
      .each(Ops.LOG_DEBUG,
	    LOGGER,
	    new Expr(Ops.TO_STRING, "root element: ", "*elem"))
      .anchor("rootNode")

      .unify("rootNode", "otherNodes")
      .each(Ops.LOG_DEBUG,
	    LOGGER,
	    new Expr(Ops.TO_STRING, "element: ", "*elem"))
      .each(Ops.IDENTITY, "*elem").out(elementsVar)
      ;
  }

  public static void addObject(MBR bounds, Long objectId, OutputCollector collector) {
    collector.emit(new AddObject(bounds, objectId));
  }

  /** Functional interface for expected dataConverter signature */
  public interface RTreeConvertorFunction<T>  extends RamaSerializable {
    public void invoke(T data, RTreeCollector collector);
  }

  /* Explode the contents of var.
     Var can refer to a microbatch, or a temporary pstate.
  */
  private Block explode(final String var, final String outVar) {
    if (var.startsWith("*")) {
      return
          Block
          .directPartition(new Expr(Ops.CURRENT_TASK_ID))
          .explodeMicrobatch(var).out(outVar);
    } else {
      return Block
          .allPartition()
          .localSelect(var, Path.all()).out(outVar);
    }
  }

  public <T> Block handleModifications(final String microbatchVar,
                                       final RTreeConvertorFunction<T> dataConvertor) {
    final String modTableVar = Helpers.genPStateVar("$$mods");
    final String nodeChangeTableVar = Helpers.genPStateVar("$$leaves");

    final String dataVar = Helpers.genVar("*data");
    final String modificationVar = Helpers.genVar("*modification");
    final String opVar = Helpers.genVar("*op");
    // final String leafNodeVar = Helpers.genVar("leafNode");
    final String leafNodeIsRootVar = Helpers.genVar("leafNodeIsRoot");
    final String rootNodeVar = Helpers.genVar("rootNode");
    final String rootNodeIdVar = Helpers.genVar("rootNodeId");

    final String nodeOpsVar = Helpers.genVar("nodeOps");
    final String nodeVar = Helpers.genVar("node");
    final String opsVar = Helpers.genVar("ops");

    // TODO hash by node id to start with.
    // TODO don't need genVar's for names inside a single batch block
    return Block
        // can't have batch block inside batch block.
        .each(Ops.CURRENT_TASK_ID).out("*taskId")
        .batchBlock(
          Block
          .each(Ops.LOG_DEBUG, LOGGER, "handleModifications")
          .macro(rootNode(rootNodeVar))
          .macro(explode(microbatchVar, "*data"))
          .each(Ops.PRINTLN, "DATA", "*data")
          .each((T data, OutputCollector collector) -> {
              RTreeCollector c = new RTreeCollector(collector);
              dataConvertor.invoke(data, c);
            },
            "*data").out("*modification")

          .each(Ops.PRINTLN, "Modification", "*modification")
          .macro(extractJavaFields("*modification", "*bounds", "*objectId"))
          // Find the node where this would be locates
          // TODO parallel descent?
          // TODO make this return the nodeId
          .macro(chooseLeaf(rootNodeVar,
                            "*bounds",
                            nodeVar,
                            leafNodeIsRootVar))
          .macro(RamaAssert.assertMacro((Node v) -> {return v != null; },
                                        nodeVar))
          // NOTE assumes chooseLeaf emits on nodeVar's partition
          .directPartition("*taskId")
          .compoundAgg(
            CompoundAgg.map(
              new Expr(Node::nodeId, nodeVar),
              Agg.list("*modification"))).out(nodeChangeTableVar))

        // Perform the insertion, looping to insert changes into parent nodes
        .loop(
            Block
            .yieldIfOvertime()
            .each(Ops.LOG_DEBUG, LOGGER, "handleModifications loop body start")
            .batchBlock(
              Block
              .allPartition()
              .localSelect(nodeChangeTableVar, Path.stay()).out("*elems")
              .each(Ops.SIZE, "*elems").out("*size")
              .each(Ops.LOG_DEBUG,
                    LOGGER,
                    new Expr(Ops.TO_STRING,
                             "size: ", "*size",
                             ", elems: ", "*elems"))
              .globalPartition()
              .agg(Agg.max("*size")).out("$$maxSize"))
            .localSelect("$$maxSize", Path.stay()).out("*maxSize")
            .each(Ops.LOG_DEBUG,
                  LOGGER,
                  new Expr(Ops.TO_STRING, "maxSize: ", "*maxSize"))
            .ifTrue(
              new Expr(Ops.EQUAL, 0, "*maxSize"),
              // Nothing left to do, all modifications handled.
              Block
              .each(Ops.LOG_DEBUG, LOGGER, "Operations loop complete, emitting")
              .emitLoop(),
              // Perform changes to next node
              Block
              .batchBlock(
                Block
                .localSelect(nodeChangeTableVar, Path.all()).out(nodeOpsVar)
                .each(Ops.LOG_DEBUG,
                      LOGGER,
                      new Expr(Ops.TO_STRING, "nodeOpsVar:", nodeOpsVar))
                .each(Ops.FIRST, nodeOpsVar).out("*opNodeId")
                .each(Ops.LAST, nodeOpsVar).out("*nodeOpsList")

                .macro(readNode("*opNodeId", "*nodesNode"))
                .ifTrue(
                  new Expr(Ops.IS_NULL, "*nodesNode"),
                  // TODO add assert that the root node has the correct node id
                  Block.directPartition("*taskId").macro(rootNode(nodeVar)),
                  Block.each(Ops.IDENTITY, "*nodesNode").out(nodeVar))
                .each(Ops.LOG_DEBUG,
                      LOGGER,
                      new Expr(Ops.TO_STRING,
                               "opNodeId: ", "*opNodeId",
                               ", nodesNode: ", "*nodesNode"))
                .each(Ops.LOG_DEBUG,
                      LOGGER,
                      new Expr(Ops.TO_STRING, "nodeVar: ", nodeVar))
                .each(Ops.LOG_DEBUG,
                      LOGGER,
                      new Expr(Ops.TO_STRING, "nodeOpsList: ", "nodeOpsList"))
                // .macro(
                //   RamaAssert.assertMacro(
                //     Ops.EQUAL,
                //     "*opNodeId",
                //     new Expr(Node::nodeId, nodeVar)))
                .macro(updateNode(M,
                                  idGenerator,
                                  nodeVar,
                                  "*nodeOpsList",
                                  "*newSiblings"))
                .each(Ops.LOG_DEBUG,
                      LOGGER,
                      new Expr(Ops.TO_STRING,
                               "UpdateNodes new siblings: ", "*newSiblings"))
                .each(List<Object>::size,"*newSiblings").out("*numSiblings")
                .each(Node::isRoot, nodeVar).out("*isRoot")
                .ifTrue(new Expr(Ops.EQUAL, 0, "*numSiblings"),
                        // No splits creating new siblings
                        Block
                        .each(Ops.LOG_DEBUG, LOGGER, "no new siblings")
                        .ifTrue(new Expr(Ops.IDENTITY, "*isRoot"),
                                Block
                                .directPartition("*taskId")
                                .macro(writeRoot(nodeVar)),
                                Block
                                .each(Node::nodeId, nodeVar).out("*nodeId")
                                .macro(writeNode("*nodeId",nodeVar)))
                        .each(Ops.IDENTITY, null).out("*newOp")
                        .each(Node::parentId, nodeVar).out("*parentId")
                        .each(Ops.IDENTITY, nodeVar).out("*parent"),
                        // Have splits creating new siblings
                        Block
                        .ifTrue(new Expr(Ops.IDENTITY, "*isRoot"),
                                // the original node was root - we need a new
                                // root node.
                                Block
                                .each(Ops.LOG_DEBUG, LOGGER, "node was root")
                                .each(Node::nodeId, nodeVar).out("*nodeId")
                                .each(Node::bounds, nodeVar).out("*nodeBounds")
                                .macro(idGenerator.genId("*parentId"))
                                .each(RTree::createRootNode,
                                      "*parentId").out("*parent")
                                .each(Node::add,
                                      "*parent",
                                      "*nodeBounds",
                                      "*nodeId")
                                .hashPartition("*taskId")
                                .each(Ops.LOG_DEBUG, LOGGER,
                                      new Expr(Ops.TO_STRING,
                                               "Write new root: ", "*parent"))
				.directPartition("*taskId")
				.macro(writeRoot("*parent"))
                                .each(Node::setParentId,
                                      nodeVar, "*parentId")
                                .macro(writeNode("*nodeId",nodeVar)),
                                // the original node was not root
                                Block
                                .each(Node::nodeId, nodeVar).out("*nodeId")
                                .macro(writeNode("*nodeId", nodeVar))
                                .each(Node::parentId, nodeVar).out("*parentId")
                                .each(Ops.IDENTITY,nodeVar).out("*parent")))
                .each(Ops.LOG_DEBUG,
                      LOGGER,
                      new Expr(Ops.TO_STRING, "parent node: ", "*parent"))
                .loopWithVars(
                  LoopVars
                  .var("*siblings", "*newSiblings")
                  .var("*ops",
                       new Expr(RTree::<RTreeCollector.AddObject>emptyList)),
                  Block
                  .ifTrue(
                    new Expr(List<Node>::isEmpty, "*siblings"),
                    Block.emitLoop("*ops"),
                    Block
                    .each(RTree::firstList, "*siblings").out("*newSibling")
                    .each(Node::bounds, "*newSibling").out("*newBounds")
                    .each(Node::nodeId, "*newSibling").out("*newId")
                    .each(Node::setParentId, "*newSibling", "*parentId")
                    .each(Ops.LOG_DEBUG,
                          LOGGER,
                          new Expr(Ops.TO_STRING,
                                   "Saving sibling: ", "*newId",
                                   " ", "*newSibling"))
                    .macro(writeNode("*newId", "*newSibling"))
                    .each(RTreeCollector.AddObject::mkAddObject,
                          "*newBounds",
                          "*newId").out("*newOp")
                    .each(RTree::<Node>restList, "*siblings").out("*remaining")
                    .each(Ops.LOG_ERROR, LOGGER,
                          new Expr(Ops.TO_STRING, "Ops: ", "*ops", " ", new Expr(Ops.CLASS, "*ops")))
                    .each(Ops.LOG_ERROR, LOGGER,
                          new Expr(Ops.TO_STRING, "newOp: ", "*newOp"))
                    .each(RTree::<RTreeCollector.AddObject>conjList,
                          "*ops",
                          "*newOp").out("*newOps1")
                    .continueLoop("*remaining", "*newOps1")))
                .out("*newOps")
                .hashPartition("*taskId")
                // .compoundAgg(
                //   CompoundAgg.map(
                //     "*parentId",
                //     Agg.list("*newOp"))).out("*newChangeTable")
                // .each(Ops.LOG_DEBUG,
                //       LOGGER,
                //       new Expr(Ops.TO_STRING,
                //                "new change table: ", "*newChangeTable"))
                .ifTrue(
                  new Expr(RTree::isEmptyList, "*newOps"),
                  Block.localTransform(
                    nodeChangeTableVar,
                    // remove what we just processed
                    Path.key("*opNodeId").termVoid()),
                  Block.localTransform(
                    nodeChangeTableVar,
                    Path
                    .multiPath(
                      // remove what we just processed
                      Path.key("*opNodeId").termVoid(),
                      // add new sibling nodes
                      Path
                      .key("*parentId")
                      .nullToVal(new Expr(RTree::emptyVec))
                      .term(RTree::addAllVec, "*newOps"))))
                .localSelect(nodeChangeTableVar, Path.stay()).out("*nnn")
                .each(Ops.LOG_DEBUG,
                      LOGGER,
                      new Expr(Ops.TO_STRING, "opNodeId: ", "*opNodeId"))
                .each(Ops.LOG_DEBUG,
                      LOGGER,
                      new Expr(Ops.TO_STRING,
                               "End of loop body: ",
                               ", Parent Id: ", "*parentId",
                               ", newOps: ", "*newOps",
                               ", table: ", "*nnn")))
              .continueLoop()))
        .each(Ops.LOG_DEBUG, LOGGER,  "Loop complete")

        // Broadcast root node to all tasks
        .batchBlock(
          Block
          .macro(rootNode("*rootNode"))
          .each(Ops.LOG_DEBUG,
		LOGGER,
		new Expr(Ops.TO_STRING,
			 "Updating global partitions: ",
			 "*rootNode"))
          .macro(broadcastRootNodeValue("*rootNode")))
        ;}

  private void declareQueries(final Topologies topologies) {
    // TODO scope query names by rtree prfix
    topologies.query("objectsInBounds", "*bounds").out("*objects")
      .each(Ops.LOG_DEBUG,
	    LOGGER,
	    new Expr(Ops.TO_STRING, "search objectsInBounds: ", "*bounds"))
      .localSelect(rootPstate, Path.stay()).out("*root")
      .each(Ops.LOG_DEBUG, LOGGER,
	    new Expr(Ops.TO_STRING, "search root: ", "*root"))
      .macro(search("*bounds", "*root", "*objects"))
      .originPartition()
      .agg(Agg.list("*objects")).out("*objects");

    topologies.query("verifyTree").out("*isValid")
      .localSelect(rootPstate, Path.stay()).out("*root")
      .each(Ops.LOG_DEBUG, LOGGER,
	    new Expr(Ops.TO_STRING, "verify root: ", "*root"))
      .macro(verify("*root", "*isValid"))
      .originPartition()
      .agg(Agg.list("*isValid")).out("*isValid")
      .localSelect(rootPstate, Path.stay()).out("*root")
      .each(Ops.LOG_DEBUG, LOGGER,
	    new Expr(Ops.TO_STRING, "verify root at end: ", "*root"))
      ;

    topologies.query("dumpTree").out("*task-ids")
      .allPartition()
      .localSelect(rootPstate, Path.stay()).out("*root")
      .each(Ops.LOG_DEBUG, LOGGER,
	    new Expr(Ops.TO_STRING, "dump tree root: ", "*root"))
      .macro(dump("*root"))
      .each(Ops.CURRENT_TASK_ID).out("*task-id")
      .originPartition()
      .agg(Agg.list("*task-id")).out("*task-ids");

    topologies.query("dumpDot").out("*allElements")
      .macro(dumpDot("*elements"))
      .originPartition()
      .agg(Agg.list("*elements")).out("*allElements");
  }


  /** Declare all the pobjects required for the RTree. */
  public void declare(final Topologies topologies,
                      final MicrobatchTopology topology) {
    declarePStates(topology);
    declareQueries(topologies);
  }

}
