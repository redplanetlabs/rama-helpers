package com.rpl.rama.helpers.spatial;

import clojure.lang.PersistentTreeMap;
import clojure.lang.PersistentVector;
import clojure.lang.Counted;
import clojure.lang.LazySeq;
import clojure.lang.PersistentArrayMap;
import clojure.lang.PersistentHashMap;

import com.rpl.rama.Agg;
import com.rpl.rama.Block;
import com.rpl.rama.CompoundAgg;
import com.rpl.rama.Expr;
import com.rpl.rama.Helpers;
import com.rpl.rama.LoopVars;
import com.rpl.rama.ModuleInstanceInfo;
import com.rpl.rama.PState;
import com.rpl.rama.Path;
import com.rpl.rama.RamaSerializable;
import com.rpl.rama.SubBatch;
import com.rpl.rama.RamaModule.Topologies;
import com.rpl.rama.helpers.ModuleUniqueIdPState;
import com.rpl.rama.helpers.RamaAssert;
import com.rpl.rama.module.MicrobatchTopology;
import com.rpl.rama.ops.Ops;
import com.rpl.rama.ops.OutputCollector;
import com.rpl.rama.ops.RamaFunction1;
import com.rpl.rama.impl.NativeRamaFunction0;
import com.rpl.rama.impl.Util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.rpl.rama.helpers.TopologyUtils.extractJavaFields;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RTree implements RamaSerializable {
  private final int dimensions;
  private final ModuleUniqueIdPState idGenerator;
  private final String nodesPstate;
  private final String rootPstate;

  private final int M;
  private final int m;

  private static final Logger LOGGER = LoggerFactory.getLogger(RTree.class);

  public static final NativeRamaFunction0 CURRENT_EVENT_NUM =
    new NativeRamaFunction0(Util.getVarContents("rpl.rama.distributed.core",
                                                    "current-event-num"));

  public static class AddObject implements RamaSerializable {
    public final MBR bounds;
    public final long objectId;

    public AddObject(final MBR bounds, long objectId) {
      this.bounds = bounds;
      this.objectId = objectId;
    }
  }

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
                           // TODO could read rootNode locally
                           final String boundsVar,
                           final String leafNodeVar) {
    final String isLeafVar = Helpers.genVar("isLeaf");
    return
        // [Initialize.] Set N to be the root node.
        // CL2. [Leaf check.] If N is a leaf, return N
        // .each(Ops.LOG_TRACE,
        //       LOGGER,
        //       new Expr(Ops.TO_STRING,
        //                "chooseLeaf root isLeaf: " , isLeafVar))


        // CL3. [Choose subtree.] If Af is not a leaf, let F be the
        // entry in N whose rectangle F.I needs least enlargement to
        // include E.I. Resolve ties by choosing the entry with the
        // rectangle of smallest area
        Block
        .loopWithVars(
          LoopVars.var("*theNode", rootNodeVar),
          // CL4. [Descend until a leaf is reached.] Set N to be the
          // child node pointed to by F.p and repeat from CL2.
          Block
          .each(Node::isLeaf, "*theNode").out(isLeafVar)
          .each(Node::nodeId, "*theNode").out("*nodeId")
          .each(Ops.LOG_DEBUG, LOGGER, "chooseLeaf loop, node: {}", "*theNode")
          .ifTrue(isLeafVar,
                  // We have reached a leaf node, so emit it
                  Block.emitLoop("*theNode"),
                  // Not at a leaf node yet.
                  Block
                  .each(Node::chooseChild,
                        "*theNode",
                        boundsVar).out("*childId")
                  .macro(readNode("*childId", "*childNode"))
                  // .each(Ops.LOG_TRACE,
                  //       LOGGER,
                  //       new Expr(Ops.TO_STRING,
                  //                "Chosen child: ", "*childNode"))
                  .continueLoop("*childNode")))
        .out(leafNodeVar)
        // .each(Ops.LOG_TRACE,
        //       LOGGER,
        //       new Expr(Ops.TO_STRING, "Chosen leaf: ", leafNodeVar))
        .macro(RamaAssert.assertMacro(Node::isLeaf, leafNodeVar))
        // .each(Ops.LOG_TRACE, LOGGER, "chooseLeaf done")
        ;
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

  /** Write the root node value to all partitions */
  private Block broadcastRootNodeValue(final String rootNodeVar) {
    return Block
        .each(Ops.LOG_ERROR, LOGGER, "broadcastRootNodeValue")
        .macro(RamaAssert.assertMacro(
          new Expr(Ops.EQUAL, 0, new Expr(Ops.CURRENT_TASK_ID)),
          "AAA"))
        .allPartition()
        .macro(writeRoot(rootNodeVar))
        // TODO can this be removed?
        .each(Ops.IDENTITY, rootNodeVar).out("*d");
  }

  private Block latestRootNode() {
    return Block
        .each(Ops.LOG_DEBUG, LOGGER, "latestRootNode")

        .localSelect("$$rootUpdate", Path.stay()).out("*nodeId")

        .ifTrue(
          new Expr(Ops.IS_NOT_NULL, "*nodeId"),
          Block
          // .each(Ops.LOG_DEBUG, LOGGER, "latestRootNode _C")
          .each(Ops.LOG_DEBUG, LOGGER, "Root node {}", "*nodeId")
          .macro(readNode("*nodeId", "*node"))
          .directPartition(0)
          .localTransform(rootPstate, Path.termVal("*node")))
        // .each(Ops.LOG_DEBUG, LOGGER, "latestRootNode done")

        // // .materialize().out("$$rootNodes")
        // .allPartition()
        // // .localTransform("$$rootNodes", Path.termVal(Vector.empty()))
        // // .each(Ops.LOG_DEBUG, LOGGER, "latestRootNode preAgg")
        // .localSelect("$$rootUpdate", Path.stay()).out("*nodeId")
        // // .each(Ops.LOG_DEBUG, LOGGER,
        // //       "latestRootNode preAgg nodeId {}", "*nodeId")
        // .keepTrue(new Expr(Ops.IS_NOT_NULL, "*nodeId"))
        // .macro(readNode("*nodeId", "*node"))
        // .each(Ops.LOG_DEBUG, LOGGER, "read node returned: {}", "*node")
        // .keepTrue(new Expr(Node::isRoot, "*node"))
        // // .each(Ops.LOG_DEBUG, LOGGER, "latestRootNode preAgg complete")
        // .globalPartition()
        // // .each(Ops.LOG_DEBUG, LOGGER, "latestRootNode Global Partition")
        // // .each(Ops.LOG_DEBUG, LOGGER,
        // //       "latestRootNode Global Partition: node {}", "*node")
        // .agg(Agg.list("*node")).out("$$rootNodes")
        // // .localTransform("$$rootNodes", Path.afterElem().termVal("*node"))
        // // .each(Ops.LOG_DEBUG, LOGGER, "latestRootNode A2")
        // .localSelect("$$rootNodes", Path.stay()).out("*nodes")
        // // .each(Ops.LOG_DEBUG, LOGGER, "latestRootNode AA {}", "*nodes")
        // .each(Counted::count, "*nodes").out("*numRoots")
        // // .each(Ops.LOG_DEBUG, LOGGER, "latestRootNode B {}", "*numRoots")
        // .ifTrue(
        //   // TODO is this gaurd necessary?
        //   new Expr(Ops.IS_POSITIVE, "*numRoots"),
        //   Block
        //   // .each(Ops.LOG_DEBUG, LOGGER, "latestRootNode _C")
        //   .each(Ops.LOG_DEBUG, LOGGER, "Root node(s) {}", "*nodes")
        //   .macro(RamaAssert.assertMacro(new Expr(Ops.EQUAL, 1, "*numRoots"),
        //                                 "one root"))
        //   // .each(Ops.LOG_DEBUG, LOGGER, "latestRootNode C")
        //   .each(Vector::peek, "*nodes").out("*firstNode")
        //   // .each(Ops.LOG_DEBUG, LOGGER, "latestRootNode D")
        //   .localTransform(rootPstate, Path.termVal("*firstNode")))
        // // .each(Ops.LOG_DEBUG, LOGGER, "latestRootNode done")
        ;
  }

  /** Broadcast root node to all tasks */
  private Block propagateRootNode() {
    return Block
        .each(Ops.LOG_ERROR, LOGGER, "propagateRootNode")
        .macro(rootNode("*rootNode"))
        // .each(Ops.LOG_TRACE,
        //       LOGGER,
        //       new Expr(Ops.TO_STRING,
        //                "Updating global partitions: ",
        //                "*rootNode"))
        .macro(broadcastRootNodeValue("*rootNode"));
  }

  private LeafNode constructRoot(long id) {
      return new LeafNode(id, id);
  }

  /** Set rootNodeVar to be the local copy of the root node.
      If the root node does not exist it is created.
   */
  private Block rootNode(final String rootNodeVar) {
    // final String currentRootNodeVar = Helpers.genVar("rootNode");
    // final String rootNodeIdVar = Helpers.genVar("rootNodeId");
    // final String taskIdVar = Helpers.genVar("taskId");
    return Block
        .localSelect(rootPstate, Path.stay()).out(rootNodeVar)
        .macro(RamaAssert.assertMacro(Ops.IS_NOT_NULL, rootNodeVar));
  }

  private Block ensureRootNode() {
    final String currentRootNodeVar = Helpers.genVar("rootNode");
    final String rootNodeVar = Helpers.genVar("rootNode");
    final String rootNodeIdVar = Helpers.genVar("rootNodeId");
    final String taskIdVar = Helpers.genVar("taskId");
    return Block
        .localSelect(rootPstate, Path.stay()).out(currentRootNodeVar)
        .ifTrue(new Expr(Ops.IS_NULL, currentRootNodeVar),
                Block
                .each(Ops.LOG_DEBUG, LOGGER, "Creating root node")
                .macro(idGenerator.genId(rootNodeIdVar))
                .each(RTree::constructRoot,
                      this,
                      rootNodeIdVar).out(rootNodeVar)
                .macro(writeNode(rootNodeIdVar, rootNodeVar))
                .localTransform("$$rootUpdate", Path.termVal(rootNodeIdVar))
                // .macro(RamaAssert.assertMacro(
                //   new Expr(Ops.EQUAL, 0, new Expr(Ops.CURRENT_TASK_ID)),
                //   "AA")),
                .macro(writeRoot(rootNodeVar))
                .macro(broadcastRootNodeValue(rootNodeVar)),
                Block
                .each(Ops.IDENTITY, currentRootNodeVar).out(rootNodeVar))
        // .each(Ops.LOG_DEBUG, LOGGER, "broadcastRootNodeValue")
        // .allPartition()
        ;
  }

  protected Block readNode(final String nodeIdVar, final String nodeVar) {
    return Block
        .each(Ops.LOG_DEBUG, LOGGER,
              new Expr(Ops.TO_STRING, "readNode: ", nodeIdVar))
        .hashPartition(nodeIdVar)
        .localSelect(nodesPstate, Path.key(nodeIdVar)).out(nodeVar)
        .each(Ops.LOG_DEBUG, LOGGER, "readNode: {} {}", nodeIdVar, nodeVar);
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
      .each(Ops.LOG_ERROR, LOGGER,
            new Expr(Ops.TO_STRING, "writeRoot: ", nodeVar))
      .localTransform(rootPstate, Path.termVal(nodeVar));
  }

  protected Block writeRootUpdate(final String nodeIdVar) {
    final String taskIdVar = Helpers.genVar("taskId");
    return Block
        .each(Ops.LOG_ERROR, LOGGER, "rootUpdate: {}", nodeIdVar)
        .each(Ops.CURRENT_TASK_ID).out(taskIdVar)
        .directPartition(0)
        .localTransform("$$rootUpdate", Path.termVal(nodeIdVar))
        .directPartition(taskIdVar);
  }

  private static PersistentVector allChildren(
    final Node currentNode,
    final List<ModificationCollector.AddObject> nodeOps) {
    return
        nodeOps
        .stream()
        .reduce(currentNode.children,
                (PersistentVector childVec, ModificationCollector.AddObject op) ->
                Vector.conj(childVec, new Child(op.bounds, op.objectId)),
                (PersistentVector c1, PersistentVector c2) ->
                Vector.into(c1, c2));
  }

  /** just append children in he order that they are appended */
  private  PersistentVector naiveGroupChildren(
    final PersistentVector children) {
    LOGGER.debug("naiveGroupChildren: " + children.toString());
    return Vector.partitionAll(M, children);
  }

  /** Linear algorithm from original R-Tree paper */
  private  PersistentVector linearGroupChildren(
    final PersistentVector children) {
    LOGGER.debug("linearGroupChildren: " + children.toString());
    // TODO implement linear
    return Vector.partitionAll(M, children);
  }


  private int srtSlices(int numPages) {
    double x;
    switch(M) {
      case 2: x = Math.sqrt(numPages); break;
      case 3: x = Math.cbrt(numPages); break;
      default: x = Math.pow(numPages, 1.0/M); break;
    }
    return (int)Math.ceil(x);
  }

  private PersistentVector strGroupChildren(PersistentVector children) {
    return strGroupChildren0(
      ((Child)children.peek()).bounds.dimensions(),
      children);
  }

  /** STR algorithm */
  private PersistentVector strGroupChildren0(
    int dimension,
    PersistentVector children) {
    // LOGGER.trace("strGroupChildren0 dimension: " + dimension + " M=" + M);
    // LOGGER.trace("strGroupChildren0 children: " + children);
    int numChildren = children.size();
    int numPages /* P */ = (int)Math.ceil(numChildren/M);
    int numSlices /* S */ = Math.max(1, srtSlices(numPages));
    // LOGGER.trace("strGroupChildren0 numPages: " + numPages +
    //              " numSlices: " + numSlices);
    children = Vector.into(
      Vector.empty(),
      ((List<Child>)children)
      .stream()
      .sorted(Comparator.comparing(
        (Child child) ->
        (Double)child.bounds.getCenter(child.bounds.dimensions() - dimension)))
      .collect(Collectors.toList()));
    // LOGGER.trace("STR A children: " + children);
    PersistentVector unsortedSlices =
         // create numSlices partitions
         Vector.partitionAll(numChildren/numSlices, children);
    // LOGGER.trace("STR AA unsortedSlices: " + unsortedSlices);
    // LOGGER.trace("STR AA unsortedSlice sizes: " +
    //              ((Collection<LazySeq>)unsortedSlices)
    //              .stream()
    //              .map(LazySeq::size)
    //              .collect(Collectors.toList()));

    PersistentVector slices =
        Vector.into(
          Vector.empty(),
          ((Collection<LazySeq>)unsortedSlices)
          .stream()
          // partition each slice by M, and flatten
          .map((LazySeq slice) ->
               {
                 // LOGGER.trace("STR B slice: " + slice);
                 if (dimension > 2) {
                   return strGroupChildren0(
                     dimension - 1,
                     Vector.into(Vector.empty(), slice));
                 } else {
                   return Vector.into(
                   Vector.empty(),
                   ((Collection<Child>)slice)
                   .stream()
                   .sorted(Comparator.comparing(
                     (Child child) ->
                     (Double)child.bounds.getCenter(
                       child.bounds.dimensions() - dimension + 1)))
                   .collect(Collectors.toList()));}
               })
          .collect(Collectors.toList()));

    PersistentVector result =
        ((Collection<PersistentVector>)slices)
        .stream()
        .reduce(Vector.empty(),
                (PersistentVector res, PersistentVector slice) ->
                Vector.into(res, Vector.partitionAll(M, slice)),
                (PersistentVector res, PersistentVector other) ->
                Vector.into(res, other));

    return result;
  }

  private Object updateNodeChildren(Node node, List nodeOps) {
    PersistentVector allChildren = allChildren(node, nodeOps);
    PersistentVector groupedChildren = strGroupChildren(allChildren);
    // Create as many siblings as needed.
    int numGroups = groupedChildren.size();

    List<INode> newSiblings = Stream.generate(() -> node.newSibling())
                              .limit(numGroups - 1)
                              .collect(Collectors.toList());

    // LOGGER.trace("updateNodeChildren size: " +
    //              newSiblings.size() + ", " +
    //              groupedChildren.size());
    node.children = Vector.into(Vector.empty(), (List)groupedChildren.get(0));
    // LOGGER.trace("size: "+ newSiblings.size() + ", " + groupedChildren.size());
    for (int i=0; i< newSiblings.size(); ++i) {
      Node currentNode = (Node)(newSiblings.get(i));
      // LOGGER.trace("i: "+ i);
      PersistentVector children = Vector.into(Vector.empty(),
                                              (List)groupedChildren.get(i + 1));
      currentNode.children = children;
    }

    newSiblings.add(0, node);

    return Arrays.asList(
      node.isRoot() && numGroups>1 ? null : node.parent,
      newSiblings);
  }

  /** Perform all operations from nodeOpsVar on nodeVar.

      This does not persist the updated node, or the new siblings.

      newSiblingIdVar will contain any new sibling nodes that have been created.
   */

  protected Block updateNode(
    final int branchingFactor,
    final ModuleUniqueIdPState idGenerator,
    final String nodeVar,
    final String nodeOpsVar,
    // outputs
    final String parentIdVar,
    final String parentOpVar) {

    final String taskIdVar = Helpers.genVar("taskId");

    // this could just be a java function, except for the id-gen
    return Block
        // Stuff the original node and siblings using the sorted children.
        .each(Ops.CURRENT_TASK_ID).out(taskIdVar)
        .each(RTree::updateNodeChildren, this, nodeVar, nodeOpsVar)
        .out("*tuple")

        // .each(Ops.EXPAND, "*tuple").out(parentIdVar, "*newSiblingsList")

        // .macro(RamaAssert.assertMacro(
        //   new Expr(Ops.IS_NOT_NULL, parentIdVar),
        //   "parentId exists"))

        // .each(Ops.EXPAND, "*tuple").out("*newParentId", "*newSiblingsList")
        // .ifTrue(new Expr(Ops.IS_NULL, "*newParentId"),
        //         Block
        //         .macro(idGenerator.genId(parentIdVar))
        //         .localTransform("$$rootUpdate", Path.termVal(parentIdVar))
        //         .each(RTree::createRootNode, parentIdVar).out("*newRootVar")
        //         .each(Ops.LOG_DEBUG, LOGGER,
        //               "new Root {} {}", parentIdVar, "*newRootVar")
        //         .macro(writeNode(parentIdVar, "*newRootVar"))
        //         .macro(writeRoot("*newRootVar")),
        //         Block.each(Ops.IDENTITY, "*newParentId").out(parentIdVar))

        .each(Ops.EXPAND, "*tuple").out("*newParentId", "*newSiblingsList")
        .ifTrue(new Expr(Ops.IS_NULL, "*newParentId"),
            Block
                .each(Ops.LOG_DEBUG, LOGGER,
                      "[{}] new Root for node {} with ops {}",
                      taskIdVar, nodeVar, nodeOpsVar)
                .macro(idGenerator.genId(parentIdVar))
                // .localTransform("$$rootUpdate", Path.termVal(parentIdVar))
                .macro(writeRootUpdate(parentIdVar))
                // TODO check we actually need to write this here
                .each(RTree::createRootNode, parentIdVar).out("*newRootVar")
                .each(Ops.LOG_DEBUG, LOGGER,
                      "[{}] new Root {} {}",
                      taskIdVar, parentIdVar, "*newRootVar")
                .macro(writeNode(parentIdVar, "*newRootVar"))
                // .macro(writeRoot("*newRootVar"))
                .each(Node::setParentId, nodeVar, parentIdVar)
                ,
                Block.each(Ops.IDENTITY, "*newParentId").out(parentIdVar))

        .each(Ops.LOG_DEBUG, LOGGER, "[{}] Explode siblings {}",
              taskIdVar, "*newSiblingsList")
        .each(List<Node>::size, "*newSiblingsList").out("*numSiblings")
        // TODO remove this check completely?
        .each(Ops.GREATER_THAN, "*numSiblings", 0 /*1*/).out("*needsOps")
        .each(Ops.EXPLODE,"*newSiblingsList").out("*sibling")
        .each(Ops.LOG_DEBUG, LOGGER,
              "[{}] Explode sibling: {}",
              taskIdVar, "*sibling")
        .each(Node::nodeId, "*sibling").out("*existingNodeId")
        .each(Ops.LOG_DEBUG, LOGGER,
              "[{}] Explode siblings A {}",
              taskIdVar, "*existingNodeId")
        .ifTrue(new Expr(Ops.IS_NULL, "*existingNodeId"),
                Block
                .macro(idGenerator.genId("*newId"))
                .each(Node::setNodeId, "*sibling", "*newId"),
                Block.each(Ops.IDENTITY, "*existingNodeId").out("*newId"))
        .each(Ops.LOG_DEBUG, LOGGER,
              "[{}] Explode write sibling {}", taskIdVar, "*sibling")
        .hashPartition("*newId")
        .each(Ops.LOG_DEBUG, LOGGER, "[{}] Set sibling parent {}",
              taskIdVar, parentIdVar)
        .each(Node::setParentId, "*sibling", parentIdVar)
        .each(Ops.LOG_DEBUG, LOGGER, "[{}] DDD", taskIdVar)
        .localTransform(nodesPstate,
                        Path
                        .key("*newId")
                        .termVal("*sibling"))
        .each(Ops.LOG_DEBUG, LOGGER, "[{}] EEE", taskIdVar)
        .each(Node::unionBounds, "*sibling").out("*newBounds")
        .each(Ops.LOG_DEBUG, LOGGER, "[{}] FFF", taskIdVar)
        .ifTrue("*needsOps",
                Block.each(ModificationCollector.AddObject::mkAddObject,
                           "*newBounds",
                           "*newId").out(parentOpVar),
                Block.each(Ops.IDENTITY, null).out(parentOpVar))
        .each(Ops.LOG_DEBUG, LOGGER, "[{}] GGG", taskIdVar)
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
  private Block search(final String boundsVar,
                       final String rootVar,
                       final String outVar) {
    final String isLeafVar = Helpers.genVar("isLeaf");
    return Block
        .loopWithVars(
          LoopVars.var("*searchNode", rootVar),
          Block
          .yieldIfOvertime()
          .each(Node::isLeaf, "*searchNode").out(isLeafVar)
          .each(Ops.LOG_DEBUG,
                LOGGER,
                new Expr(Ops.TO_STRING, "search isLeaf: ", isLeafVar))
          .ifTrue(new Expr(Ops.EQUAL, isLeafVar, false),
                  // S1. [Search subtrees.] If T is not a leaf, check each entry E to
                  // determine whether E.I overlaps S. For all overlapping entries, invoke
                  // Search on the tree whose root node is pointed to by E.p .
                  Block
                  .each(Node::overlapping,
                        "*searchNode",
                        boundsVar).out("*childIds")
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
                  .each(Ops.LOG_DEBUG, LOGGER, "Single leaf node. searchNode {}",
                        "*searchNode")
                  .each(Node::overlapping, "*searchNode", boundsVar).out("*ids")
                  .each(Ops.LOG_DEBUG, LOGGER, "Matching objects {}", "*ids")
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

  private static List<Object> restList(List<Object> l) {
    l.remove(0);
    return l;
  }

  private static List<Object> conjList(List<Object> l, Object n) {
    // LOGGER.trace("conjList " + n.toString());
    l.add(n);
    return l;
  }

  private static <T> List<T> conjList1(List<T> l, T n) {
    // LOGGER.trace("conjList " + n.toString());
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

  private Block verify(final String rootVar, final String outVar) {
    return Block
        .loopWithVars(
          LoopVars.var("*vNode", rootVar),
          Block
          .yieldIfOvertime()
          .ifTrue(new Expr(Node::isLeaf, "*vNode"),
                  Block.emitLoop(true),

                  Block
                  .each(Node::getChildren, "*vNode").out("*children")
                  .each(Node::unionBounds, "*vNode").out("*bounds")
                  .each(MBR::empty, dimensions).out("*emptyMBR")
                  .each(RTree::<Node>emptyList).out("*emptyChildrenNodes")
                  .loopWithVars(
                    LoopVars
                    .var("*children", "*children")
                    .var("*totalBounds", "*emptyMBR")
                    .var("*childrenNodes", "*emptyChildrenNodes"),
                    Block
                    .ifTrue(
                      new Expr(PersistentVector::isEmpty, "*children"),
                      Block.emitLoop("*totalBounds", "*childrenNodes"),
                      Block
                      .each(Vector::peek, "*children").out("*child")
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
                      .continueLoop(new Expr(Vector::pop, "*children"),
                                    new Expr(MBR::union,
                                             "*totalBounds",
                                             "*bounds"),
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
        .localSelect(nodesPstate, Path.all()).out("*dumpNode")
        .each(Ops.LOG_INFO,
              LOGGER,
              new Expr(Ops.TO_STRING, "Node: ", "*dumpNode"));
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

  /** Dump bounding boxes for display */
  private Block dumpBounds(final String elementsVar) {
    return Block
        .localSelect(rootPstate, Path.stay()).out("*rootNode")
        .each(Ops.LOG_DEBUG, LOGGER, "dumpBounds Root node: {}", "*rootNode")
        .ifTrue(
          new Expr(Ops.IS_NULL, "*rootNode"),
          Block.each(Ops.EXPLODE,
                     new Expr(RTree::<Object>emptyList)).out(elementsVar),
          Block.loopWithVars(
            LoopVars
            .var("*node", "*rootNode")
            .var("*level", 0),
            Block
            .each(Ops.EXPLODE, new Expr(Node::getChildren, "*node")).out("*child")
            .emitLoop(new Expr(Ops.TUPLE,
                               "*level",
                               new Expr(Child::boundsString, "*child")))
            .ifTrue(
              new Expr(Ops.NOT, new Expr(Node::isLeaf, "*node")),
              Block
              .each(Child::getId, "*child").out("*childId")
              .macro(readNode("*childId", "*nextNode"))
              .continueLoop("*nextNode", new Expr(Ops.INC, "*level"))))
          .out(elementsVar));
  }

/** Dump bounding boxes for display */
  private Block boundsStats(final String statsVar) {
    return Block
        .localSelect(rootPstate, Path.stay()).out("*rootNode")
        .ifTrue(
          new Expr(Ops.IS_NULL, "*rootNode"),
          Block.each(Ops.EXPLODE,
                     new Expr(RTree::<Object>emptyList)).out(statsVar),
          Block.loopWithVars(
            LoopVars
            .var("*node", "*rootNode")
            .var("*level", 0),
            Block
            .emitLoop(new Expr(Ops.TUPLE,
                               "*level",
                               new Expr(Node::overlapArea, "*node")))
            .each(Ops.EXPLODE, new Expr(Node::getChildren, "*node")).out("*child")
            .ifTrue(
              new Expr(Ops.NOT, new Expr(Node::isLeaf, "*node")),
              Block
              .each(Child::getId, "*child").out("*childId")
              .macro(readNode("*childId", "*nextNode"))
              .continueLoop("*nextNode", new Expr(Ops.INC, "*level"))))
          .out(statsVar));
  }

  public static void addObject(MBR bounds, Long objectId, OutputCollector collector) {
    collector.emit(new AddObject(bounds, objectId));
  }

  /** Functional interface for expected dataConverter signature */
  public interface ModificationConvertorFunction<T>  extends RamaSerializable {
    public void invoke(T data, ModificationCollector collector);
  }

  /* Explode the contents of var.
     Var can refer to a microbatch, or a temporary pstate.
  */
  private Block explode(final String var, final String outVar) {
    if (var.startsWith("*")) {
      return
          Block
          .explodeMicrobatch(var).out(outVar);
    } else {
      return Block
          .allPartition()
          .localSelect(var, Path.all()).out(outVar);
    }
  }

  private static PersistentTreeMap emptySortedMap() {
    return new PersistentTreeMap();
  }

  private <T> Block buildModTable(
    final String userModTableVar,
    final ModificationConvertorFunction<T> dataConvertor,
    final String modTableVar,
    final String rootUpdateVar) {

    final String taskIdVar = "*taskId";

    return Block
        .each(Ops.LOG_DEBUG, LOGGER, "buildModTable")
        .allPartition()

        // TODO delete these
        .each(RTree::emptySortedMap).out("*emptyMap")
        .localTransform(modTableVar, Path.termVal("*emptyMap"))

        .localSelect(userModTableVar, Path.all()).out("*data")
        .each(Ops.LOG_DEBUG, LOGGER, "DATA {}", "*data")
        .each((T data, OutputCollector collector) -> {
            ModificationCollector c = new ModificationCollector(collector);
            dataConvertor.invoke(data, c);
          },
          "*data").out("*modification")

        .each(Ops.LOG_DEBUG, LOGGER, "Modification {}", "*modification")
        // .each(Ops.LOG_DEBUG, LOGGER,
        //       new Expr(Ops.TO_STRING, "Modification ", "*modification"))

        // TODO extractJavaFields is not very efficient
        .macro(extractJavaFields("*modification", "*bounds", "*objectId"))

        // Find the node where this would be located
        // TODO parallel descent?
        .macro(rootNode("*rootNode"))
        .macro(chooseLeaf("*rootNode", "*bounds", "*chosenNode"))
        .macro(RamaAssert.assertMacro((Node v) -> {return v != null; },
                                      "*chosenNode"))
        // NOTE assumes chooseLeaf emits on *node's partition
        .each(ModTableKey::mkLeafKey,
              new Expr(Node::nodeId, "*chosenNode")).out("*tmpKey")

        .each(Node::nodeId, "*chosenNode").out("*nodeId")
        .hashPartition("*nodeId")
        .localTransform(
          modTableVar,
          Path
          .key("*tmpKey")
          .nullToList()
          .afterElem()
          .termVal("*modification"))
        ;
  }

  private Block hasNoMoreModsPred(final String modTableVar,
                                  final String noMoreOpsVar) {
    return Block
        // .each(Ops.LOG_DEBUG, LOGGER, "hasNoMoreModsPred")
        .macro(RamaAssert.assertMacro(
          new Expr(Ops.EQUAL, 0, new Expr(Ops.CURRENT_TASK_ID)),
          "BB"))
        .batchBlock(
            Block
            .allPartition()
            .localSelect(modTableVar, Path.mapVals()).out("*elems")
            .each(Ops.SIZE, "*elems").out("*size")
            .each(Ops.LOG_DEBUG,
                  LOGGER,
                  "hasNoMoreModsPred size: {}, elems {}", "*size", "*elems")
          .globalPartition()
          .agg(Agg.max("*size")).out("$$maxSize"))
        .localSelect("$$maxSize", Path.stay()).out("*maxSize")
        .each(Ops.LOG_DEBUG, LOGGER, "maxSize: {}", "*maxSize")
        .each(Ops.IDENTITY,
              new Expr(Ops.OR,
                       new Expr(Ops.IS_NULL, "*maxSize"),
                       new Expr(Ops.EQUAL, 0, "*maxSize")))
        .out(noMoreOpsVar);
  }

  private Block lookupNode(final String nodeIdVar,
                           final String nodeVar) {

    // final String tmpNodeVar = Helpers.genVar("node");
    return Block
        .each(Ops.LOG_DEBUG, LOGGER, "lookuNode {}", nodeIdVar)
        .macro(readNode(nodeIdVar, nodeVar))
        .each(Ops.LOG_DEBUG, LOGGER, "lookuNode {} {}", nodeIdVar, nodeVar)
        .macro(RamaAssert.assertMacro(
          new Expr(Ops.IS_NOT_NULL, nodeVar),
          "node exists"))

        // .ifTrue(
        //   new Expr(Ops.IS_NOT_NULL, tmpNodeVar),
        //   Block.each(Ops.IDENTITY, tmpNodeVar).out(nodeVar),
        //   Block
        //   // .macro(RamaAssert.assertMacro(new Expr(Ops.IDENTITY, false),
        //   //                               "shouldnt get here"))
        //   .each(Ops.LOG_DEBUG, LOGGER, "new root node {}", nodeIdVar)
        //   .hashPartition(nodeIdVar)
        //   .localTransform("$$rootUpdate", Path.termVal(nodeIdVar))
        //   .each(RTree::createRootNode, nodeIdVar).out(nodeVar))

          // .each(Ops.LOG_TRACE, LOGGER,
          //       new Expr(Ops.TO_STRING,
          //                "lookup nodeId: ", nodeIdVar, ", node: ", nodeVar))
        .each(Ops.LOG_DEBUG, LOGGER,
              new Expr(Ops.TO_STRING,
                       "lookupNode: ", nodeIdVar,
                       " got ", new Expr(Node::nodeId, nodeVar)))
        .macro(
          RamaAssert.assertMacro(
            new Expr(
              Ops.EQUAL,
              nodeIdVar,
              new Expr(Node::nodeId, nodeVar)),
            "Correct node id"));
  }

  private Block outputNode(final String taskIdVar, final String nodeVar) {
    return Block
        .ifTrue(new Expr(Node::isRoot, nodeVar),
                Block
                .directPartition(taskIdVar)
                .macro(writeRoot(nodeVar)),

                Block
                .each(Node::nodeId, nodeVar).out("*nodeId")
                .macro(writeNode("*nodeId", nodeVar)));
  }

  /** Class for key to enable sort by level **/
  public static class ModTableKey
      implements Comparable<ModTableKey>, Cloneable {
    public Long level;  // TODO we need this to be depth as the tree is not balanced
    public Long opNodeId;

    public ModTableKey(Long opNodeId) {
      this.level = 0L;
      this.opNodeId = opNodeId;
    }

    public ModTableKey(Long level, Long opNodeId) {
      this.level = level;
      this.opNodeId = opNodeId;
    }

    public static ModTableKey mkLeafKey(Long opNodeId) {
      return new ModTableKey(opNodeId);
    }

    public static ModTableKey mkKey(Long level, Long opNodeId) {
      return new ModTableKey(level, opNodeId);
    }


    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((level == null) ? 0 : level.hashCode());
      result = prime * result + ((opNodeId == null) ? 0 : opNodeId.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      ModTableKey other = (ModTableKey) obj;
      if (level == null) {
        if (other.level != null)
          return false;
      } else if (!level.equals(other.level))
        return false;
      if (opNodeId == null) {
        if (other.opNodeId != null)
          return false;
      } else if (!opNodeId.equals(other.opNodeId))
        return false;
      return true;
    }

    @Override
    public int compareTo(ModTableKey other) {
      if (other == null) {
        return 1;
      }

      // First compare by level
      int levelComparison = Long.compare(this.level, other.level);
      if (levelComparison != 0) {
        return levelComparison;
      }

      // If levels are equal, compare by opNodeId
      return Long.compare(this.opNodeId, other.opNodeId);
    }

    @Override
    public ModTableKey clone() {
      // Shallow clone: Since Long objects are immutable, a shallow clone using
      // super.clone() is sufficient
      try {
        return (ModTableKey) super.clone();
      } catch (CloneNotSupportedException e) {
        // This should never happen since we implement Cloneable
        throw new AssertionError("Clone not supported", e);
      }
    }

    @Override
    public String toString() {
      return "ModTableKey [level=" + level + ", opNodeId=" + opNodeId + "]";
    }
  }

  static Map.Entry<ModTableKey, PersistentVector> nextOps(
      Map<ModTableKey, PersistentVector> allOps) {
    TreeMap<ModTableKey, PersistentVector> m =
        new TreeMap<ModTableKey, PersistentVector>(allOps);
    return m.firstEntry();
  }

  public <T> Block handleModifications(
    final String userModTableVar,
    final ModificationConvertorFunction<T> dataConvertor) {

    final String taskIdVar = "*taskId";
    final String ptaskIdVar = "*ptaskId";

    // TODO use genVar

    return Block
        .each(Ops.LOG_ERROR, LOGGER, "handleModifications")
        .batchBlock(Block.keepTrue(false).materialize().out("$$rootUpdate"))
        .batchBlock(Block.keepTrue(false).materialize().out("$$modTable"))
        .each(Ops.LOG_DEBUG, LOGGER, "ensureRootNode")
        .batchBlock(Block.macro(ensureRootNode()))
        .each(Ops.LOG_DEBUG, LOGGER, "ensureRootNode done")

        .each(CURRENT_EVENT_NUM).out("*startEvent")
        // .each(Ops.LOG_DEBUG, LOGGER, "handleModifications before materialize")

        // .each(Ops.LOG_DEBUG, LOGGER, "handleModifications before build")
        .batchBlock(Block.macro(buildModTable(userModTableVar,
                                              dataConvertor,
                                              "$$modTable",
                                              "$$rootUpdate")))
        .each(Ops.LOG_DEBUG, LOGGER, "buildModTable finished")
        .each(CURRENT_EVENT_NUM).out("*afterBuildEvent")
        // .localSelect("$$modTable",
        //              Path.stay()
        //              .view(Counted::count)
        //              ).out("*numChanges")
        .macro(hasNoMoreModsPred("$$modTable", "*hasNoOps"))
        // .each(Ops.LOG_ERROR, LOGGER,
        //               new Expr(Ops.TO_STRING, "INITIAL TABLE: ", "*wholeTable"))
        .each(Ops.LOG_DEBUG, LOGGER, "hasNoOps: {}", "*hasNoOps")
        .ifTrue(
          new Expr(Ops.NOT, "*hasNoOps"),
          // Perform the insertion, looping to insert changes into parent nodes
          Block
          .loop(
            Block
            // .each(Ops.LOG_TRACE, LOGGER, "handleModifications loop body start")
            .macro(hasNoMoreModsPred("$$modTable", "*hasNoMoreOps"))
            // .each(Ops.LOG_DEBUG, LOGGER,
            //       new Expr(Ops.TO_STRING, "hasNoMoreOps: ", "*hasNoMoreOps"))
            .ifTrue(
              "*hasNoMoreOps",
              // Nothing left to do, all modifications handled.
              Block
              // .each(Ops.LOG_TRACE, LOGGER, "Operations loop complete, emitting")
              .emitLoop(),
              // Perform changes to next node
              Block
              .macro(RamaAssert.assertMacro(
                new Expr(Ops.EQUAL, 0, new Expr(Ops.CURRENT_TASK_ID)),
                "FF"))
              .batchBlock(
                Block
                .allPartition()
                .each(Ops.CURRENT_TASK_ID).out(taskIdVar)
                .each(Ops.LOG_DEBUG, LOGGER, "[{}] Updating nodes on task",
                      taskIdVar)
                .localSelect("$$modTable", Path.stay()).out("*abcd")
                .each(Ops.LOG_DEBUG, LOGGER,
                      "[{}] abcd {}", taskIdVar, "*abcd" )

                .localSelect("$$modTable", Path.all()).out("*nodeOps")
                // .each(Ops.LOG_DEBUG, LOGGER, "Loop body AA")
                // TODO move this destructuring into updateNode
                .each(Ops.FIRST, "*nodeOps").out("*modKey")
                .macro(extractJavaFields("*modKey", "*opNodeId", "*level"))
                .each(Ops.LAST, "*nodeOps").out("*nodeOpsList")
                .each(Ops.LOG_DEBUG, LOGGER,
                      "[{}] Updating node {} with {}",
                      taskIdVar, "*opNodeId", "*nodeOpsList" )
                .ifTrue(
                  new Expr(Ops.IS_NOT_NULL, "*opNodeId"),
                  Block
                  .macro(lookupNode("*opNodeId", "*currentNode")),
                  Block
                  .macro(idGenerator.genId("*opNodeId1"))
                  .each(Ops.LOG_DEBUG, LOGGER, "[{}] new root node {}",
                        taskIdVar, "*opNodeId1")
                  .hashPartition("*opNodeId1")
                  .localTransform("$$rootUpdate", Path.termVal("*opNodeId1"))
                  .each(RTree::createRootNode, "*opNodeId1").out("*currentNode")
                        )

                // .each(Ops.LOG_ERROR, LOGGER,
                //       new Expr(Ops.TO_STRING,
                //                "TABLE ID: ", "*opNodeId",
                //                ", level: ", "*level"))
                // .each(Ops.LOG_TRACE,
                //       LOGGER,
                //       new Expr(Ops.TO_STRING,
                //                "opNodeId: ", "*opNodeId",
                //                ", nodeOpsList: ", "*nodeOpsList"))
                .each(Ops.LOG_DEBUG, LOGGER, "[{}] Updating node {} with {}",
                      taskIdVar, "*currentNode", "*nodeOpsList")
                .macro(updateNode(M,
                                  idGenerator,
                                  "*currentNode",
                                  "*nodeOpsList",

                                  "*parentId",
                                  "*parentOp"))
                .keepTrue(new Expr(Ops.IS_NOT_NULL, "*parentOp"))
                .hashPartition("*parentId")
                .compoundAgg(
                  CompoundAgg.map(new Expr(ModTableKey::mkKey,
                                           new Expr(Ops.INC_LONG, "*level"),
                                           "*parentId"),
                                  Agg.list("*parentOp"))
                             ).out("$$newModTable")

                .each(Ops.CURRENT_TASK_ID).out(ptaskIdVar)
                .localSelect("$$newModTable", Path.stay()).out("*modsValue")
                .localTransform("$$modTable",
                                Path
                                .termVal("*modsValue"))

                .each(Ops.LOG_DEBUG, LOGGER,
                      "Node updates for task done {}",
                      "*modsValue"))
              //.batchBlock(Block.macro(propagateRootNode()))
              .continueLoop()))

          .each(Ops.LOG_ERROR, LOGGER, "propagateRootNode")
          .batchBlock(Block.macro(latestRootNode()))
          .batchBlock(Block.macro(propagateRootNode()))
          .each(Ops.LOG_DEBUG, LOGGER, "propagateRootNode done")

          // .each(Ops.LOG_TRACE, LOGGER,  "Loop complete")
          .each(CURRENT_EVENT_NUM).out("*insertEvent")

          // TODO make this conditional on it having changed
          .macro(RamaAssert.assertMacro(
            new Expr(Ops.EQUAL, 0, new Expr(Ops.CURRENT_TASK_ID)),
            "GG"))
          .each(CURRENT_EVENT_NUM).out("*endEvent")
          .each(Ops.MINUS,
                "*afterBuildEvent", "*startEvent").out("*searchEvents")
          .each(Ops.MINUS,
                "*insertEvent", "*afterBuildEvent").out("*insertEvents")
          .each(Ops.MINUS, "*endEvent", "*insertEvent").out("*rootEvents")
          .each(Ops.MINUS, "*endEvent", "*startEvent").out("*totalEvents")
          .each(Ops.LOG_INFO, LOGGER,
                new Expr (Ops.TO_STRING,
                          "Search events: ", "*searchEvents",
                          ", insert events: ", "*insertEvents",
                          ", root events: ", "*rootEvents",
                          ", Total events: ", "*totalEvents"))
          .each(Ops.LOG_ERROR, LOGGER, "handleModifications done"));
  }

  public static int partition(Object obj, int numTasks) {
    return Math.floorMod(Vector.hash(obj), numTasks);
  }

  private static ArrayList<Node> sortByPartition(
    ArrayList<Node> siblings, int numTasks) {

    siblings.sort(Comparator.comparing(
      (Node n) -> {
        return partition((Long)n.nodeId(), numTasks);
      }));
    return siblings;
  }

  private Block saveList(final String newSiblingsVar,
                         final String parentIdVar) {
    return
        Block
        .each(Ops.MODULE_INSTANCE_INFO).out("*mii1")
        .each(ModuleInstanceInfo::getNumTasks, "*mii1").out("*numTasks1")
        .allPartition()
        .each(Ops.CURRENT_TASK_ID).out("*taskIdx")
        .each(Ops.EXPLODE, newSiblingsVar).out("*newSibling")
        .each(Node::nodeId, "*newSibling").out("*newId")
        .each(Node::bounds, "*newSibling").out("*newBounds")
        .each(Node::setParentId, "*newSibling", parentIdVar)
        .macro(writeNode("*newId", "*newSibling"))
        .each(RTree::partition, "*newId", "*numTasks1").out("*partition")
        .keepTrue(new Expr(Ops.EQUAL, "*taskIdx", "*partition"))
        ;
  }

  private Block saveAndOps(final String newSiblingsVar,
                           final String newOpsVar) {
    return Block
        .each(Ops.MODULE_INSTANCE_INFO).out("*mii")
        .each(ModuleInstanceInfo::getNumTasks, "*mii").out("*numTasks")
        .each(RTree::sortByPartition,
              newSiblingsVar, "*numTasks"
              ).out("*sortedSiblings")
        .each(Ops.CURRENT_TASK_ID).out("*taskId1")
        .macro(saveList("*newSiblings", "*parentId"))
        .each(Ops.CURRENT_TASK_ID).out("*taskId2")
        .keepTrue(new Expr(Ops.EQUAL, "*taskId1", "*taskId2"))
        .loopWithVars(
          LoopVars
          .var("*siblings", "*sortedSiblings")
          .var("*ops",
               new Expr(RTree::<ModificationCollector.AddObject>emptyList)),
          Block
          .ifTrue(
            new Expr(List<Node>::isEmpty, "*siblings"),
            Block.emitLoop("*ops"),
            Block
            .each(RTree::firstList, "*siblings").out("*newSibling")
            .each(Node::bounds, "*newSibling").out("*newBounds")
            .each(Node::nodeId, "*newSibling").out("*newId")
            .each(ModificationCollector.AddObject::mkAddObject,
                  "*newBounds",
                  "*newId").out("*newOp")
            .each(RTree::<ModificationCollector.AddObject>conjList,
                  "*ops",
                  "*newOp").out("*newOps1")
            .each(RTree::restList, "*siblings").out("*remaining")
            .continueLoop("*remaining", "*newOps1")))
        .out(newOpsVar)
        ;}

  private Block updateModTable(
    final String modTableVar,
    final String opNodeIdVar,
    final String parentIdVar,
    final String levelVar,
    final String newOpsVar) {
    final String modKeyVar = Helpers.genVar("modKey");
    final String prevKeyVar = Helpers.genVar("prevKey");
    final String isEmptyVar = Helpers.genVar("isEmpty");
    return Block
        .each(ModTableKey::mkKey,
              new Expr(Ops.DEC_LONG, levelVar), opNodeIdVar).out(prevKeyVar)
        .each(ModTableKey::mkKey, levelVar, parentIdVar).out(modKeyVar)
        .each(RTree::isEmptyList, newOpsVar).out(isEmptyVar)
        .each(Ops.LOG_DEBUG, LOGGER,
              new Expr(Ops.TO_STRING,
                       "isEmpty: ", isEmptyVar,
                       ", prevKey: ", prevKeyVar,
                       ", modKey: ", modKeyVar))
        .ifTrue(
          isEmptyVar,
          Block
          .hashPartition(opNodeIdVar)
          .localTransform(
            modTableVar,
            // remove what we just processed
            Path.key(prevKeyVar).termVoid()),
          Block
          .hashPartition(opNodeIdVar)
          .localTransform(
            modTableVar,
            // remove what we just processed
            Path.key(prevKeyVar).termVoid())
          .hashPartition(parentIdVar)
          // add new sibling nodes
          .localTransform(
            modTableVar,
            Path
            .key(modKeyVar)
            .nullToVal(new Expr(Vector::empty))
            .term(Vector::into, newOpsVar)));
  }

  private void declareQueries(final Topologies topologies) {
    // TODO scope query names by rtree prfix
    topologies.query("objectsInBounds", "*bounds").out("*objects")
        .each(Ops.LOG_DEBUG,
              LOGGER,
              new Expr(Ops.TO_STRING, "search objectsInBounds: ", "*bounds"))
        .localSelect(rootPstate, Path.stay()).out("*root")
        .each(Ops.LOG_DEBUG, LOGGER,
              new Expr(Ops.TO_STRING, "search root: ", "*root"))
        // TODO change this to a guard, for the case when there is no data
        .macro(RamaAssert.assertMacro(Ops.IS_NOT_NULL, "*root"))
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

    topologies.query("dumpBounds").out("*allBounds")
        .macro(dumpBounds("*elements"))
        .originPartition()
        .agg(Agg.list("*elements")).out("*allBounds");

    topologies.query("boundsStats").out("*allStats")
        .macro(boundsStats("*elements"))
        .originPartition()
        .agg(Agg.list("*elements")).out("*allStats");
  }


  /** Declare all the pobjects required for the RTree. */
  public void declare(final Topologies topologies,
                      final MicrobatchTopology topology) {
    declarePStates(topology);
    declareQueries(topologies);
  }

}
