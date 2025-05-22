package com.rpl.rama.helpers.spatial;

import clojure.lang.PersistentVector;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.rpl.rama.helpers.TopologyUtils.extractJavaFields;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class RTree implements RamaSerializable {
  private final int dimensions;
  private final ModuleUniqueIdPState idGenerator;
  private final String nodesPstate;
  private final String rootPstate;

  private final int M;
  private final int m;

  private static final Logger LOGGER = LoggerFactory.getLogger(RTree.class);

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
                           final String boundsVar,
                           final String leafNodeVar) {
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
                .each(Ops.IDENTITY, true).out("*leafNodeIsRoot")
                .each(Ops.IDENTITY, rootNodeVar).out(leafNodeVar),

                // CL3. [Choose subtree.] If Af is not a leaf, let F be the entry in N whose
                // rectangle F.I needs least enlargement to include E.I. Resolve ties by
                // choosing the entry with the rectangle of smallest area
                Block
                .loopWithVars(
                  LoopVars.var("*theNode", rootNodeVar),
                  // CL4. [Descend until a leaf is reached.] Set N to be the child node
                  // pointed to by F.p and repeat from CL2.
                  Block
                  .yieldIfOvertime()
                  .each(Node::isLeaf, "*theNode").out(isLeafVar)
                  .each(Node::nodeId, "*theNode").out("*nodeId")
                  .each(Ops.LOG_DEBUG,
                        LOGGER,
                        new Expr(Ops.TO_STRING,
                                 "chooseLeaf loop, node: ",
                                 "*theNode"))
                  .ifTrue(
                    new Expr(Ops.EQUAL, isLeafVar, true),
                    // We have reached a leaf node, so emit it
                    Block.emitLoop("*theNode"),
                    // Not at a leaf node yet.
                    Block
                    .each(Ops.IDENTITY, false).out("*leafNodeIsRoot")
                    .each(Node::chooseChild, "*theNode", boundsVar).out("*childId")
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

  /** Write the root node value to all partitions */
  private Block broadcastRootNodeValue(final String rootNodeVar) {
    return Block
        .batchBlock(
          Block
          .allPartition()
          .macro(writeRoot(rootNodeVar)));
  }

  /** Broadcast root node to all tasks */
  private Block propagateRootNode() {
    return Block
        .macro(rootNode("*rootNode"))
        .each(Ops.LOG_DEBUG,
              LOGGER,
              new Expr(Ops.TO_STRING,
                       "Updating global partitions: ",
                       "*rootNode"))
        .macro(broadcastRootNodeValue("*rootNode"));
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
        // .each(Ops.IDENTITY, currentRootNodeVar).out(rootNodeVar)
        .each(Ops.PRINTLN, "Root node", rootNodeVar);
  }

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

  private static PersistentVector allChildren(
    final Node currentNode,
    final List<RTreeCollector.AddObject> nodeOps) {
    return
        nodeOps
        .stream()
        .reduce(currentNode.children,
                (PersistentVector childVec, RTreeCollector.AddObject op) ->
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

  /** STR algorithm */
  private  PersistentVector strGroupChildren(
    PersistentVector children) {
    LOGGER.debug("strGroupChildren: " + children.toString());
    int numNodes /* P */ = (int)Math.ceil(children.size()/M);
    int slixeSize /* S */ = (int)Math.ceil(Math.sqrt(numNodes));
    children = Vector.into(
      Vector.empty(),
      ((List<Child>)children)
      .stream()
      .sorted(Comparator.comparing(
        (Child child) -> (Double)child.bounds.getMin(0)))
      .collect(Collectors.toList()));

    return Vector.partitionAll(M, children);
  }

  private static Object updateNodeChildren(Node node,
                                           List<Node>newSiblings,
                                           PersistentVector groupedChildren) {
    LOGGER.error("updateNodeChildren size: " +
                 newSiblings.size() + ", " +
                 groupedChildren.size());
    node.children = Vector.into(Vector.empty(), (List)groupedChildren.get(0));
    LOGGER.error("size: "+ newSiblings.size() + ", " + groupedChildren.size());
    for (int i=0; i< newSiblings.size(); ++i) {
      Node currentNode = newSiblings.get(i);
      LOGGER.error("i: "+ i);
      PersistentVector children = Vector.into(Vector.empty(),
                                              (List)groupedChildren.get(i + 1));
      currentNode.children = children;
    }
    return null;
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
    final String newSiblingsVar) {

    // this could just be a java function, except for the id-gen
    return Block
        .each(Ops.LOG_DEBUG,
              LOGGER,
              new Expr(Ops.TO_STRING, "updateNode ", nodeVar, " ", nodeOpsVar))
        // NOTE - this is a temp var to avoid an array list literal in the Loop
        // var, which causes it to use the object cache.
        .each(RTree::allChildren, nodeVar, nodeOpsVar).out("*allChildren")
        .each(RTree::naiveGroupChildren, this, "*allChildren").out("*groupedChildren")
        // Create as many siblings as needed.
        .each(RTreeHelpers::newArrayList).out("*emptySiblings")
        .each(Ops.IDENTITY,
              new Expr(Ops.SIZE, "*groupedChildren")).out("*numGroups")
        .each(Ops.LOG_DEBUG, LOGGER,
              new Expr(Ops.TO_STRING, "groupedChildren size: ", "*numGroups"))
        .loopWithVars(
          LoopVars
          .var("*numNewSiblings", new Expr( Ops.DEC, "*numGroups"))
          .var("*siblings", new Expr(RTreeHelpers::newArrayList)),
          Block
          .ifTrue(
            new Expr(Ops.LESS_THAN_OR_EQUAL, "*numNewSiblings", 0),
            Block
            .each(Ops.LOG_DEBUG, LOGGER,
                  new Expr(Ops.TO_STRING, "updateNode emitting"))
            .emitLoop("*siblings"),
            Block
            .each(Ops.LOG_DEBUG, LOGGER,
                  new Expr(Ops.TO_STRING,
                           "numNewSiblings: ", "*numNewSiblings",
                           " size: ", new Expr(Ops.SIZE, "*siblings")))
            .macro(idGenerator.genId("*newNodeId"))
            .each(Node::newSibling,
                  "*currentNode",
                  "*newNodeId").out("*newNode")
            .each(RTree::conjList, "*siblings","*newNode")
            .continueLoop(new Expr(Ops.DEC, "*numNewSiblings"),
                          "*siblings"))).out(newSiblingsVar)

        // Stuff the original node and siblings using the sorted children.
        .each(RTree::updateNodeChildren,
              nodeVar,
              newSiblingsVar,
              "*groupedChildren")

        // .each(RTreeHelpers::newArrayList).out("*emptySiblings")
        // .each(Ops.LOG_DEBUG, LOGGER, "before loop")
        // .loopWithVars(
        //   LoopVars
        //   .var("*currentNode", nodeVar)
        //   .var("*nodeOpsRemaining", nodeOpsVar)
        //   .var("*siblings", "*emptySiblings"),
        //   Block
        //   .yieldIfOvertime()
        //   .each(Ops.LOG_DEBUG,
        //         LOGGER,
        //         new Expr(Ops.TO_STRING,
        //                  "start of loop, current node", "*currentNode"))
        //   .each(Ops.LOG_DEBUG,
        //         LOGGER,
        //         new Expr(Ops.TO_STRING,
        //                  "start of loop, opsRemaining", "*nodeOpsRemaining"))
        //   .ifTrue(
        //     new Expr(Ops.AND,
        //              new Expr(Node::isFull, "*currentNode", branchingFactor),
        //              new Expr(Ops.NOT,
        //                       new Expr(APersistentVector::isEmpty,
        //                                "*nodeOpsRemaining"))),
        //     Block
        //     .each(Ops.LOG_DEBUG, LOGGER, "Needs split block")
        //     .macro(idGenerator.genId("*newNodeId"))
        //     .each(Node::newSibling,
        //           "*currentNode",
        //           "*newNodeId").out("*newNode")
        //     .each((final List<Node> l, final Node node) -> {
        //         l.add(node);
        //         return l;
        //     },
        //       "*siblings",
        //       "*newNode")
        //     .continueLoop("*newNode",
        //                   "*nodeOpsRemaining",
        //                   "*siblings")
        //     ,
        //     Block
        //     .each(Ops.LOG_DEBUG, LOGGER, "no split block")
        //     .each(List<Node>::size, "*nodeOpsRemaining").out("*opCount")
        //     .ifTrue(new Expr(Ops.EQUAL, "*opCount", 0),
        //             Block
        //             .each(Ops.LOG_DEBUG, LOGGER, "Emitting")
        //             .emitLoop("*siblings"),
        //             Block
        //             .each(Ops.LOG_DEBUG, LOGGER, "more ops")
        //             .each(APersistentVector::peek,
        //                   "*nodeOpsRemaining").out("*nodeOp")
        //             .macro(extractJavaFields("*nodeOp", "*bounds", "*objectId"))
        //             .each(Node::add, "*currentNode", "*bounds", "*objectId")
        //             .continueLoop(
        //               "*currentNode",
        //               new Expr(APersistentVector::pop, "*nodeOpsRemaining"),
        //               "*siblings"))
        //           )).out(newSiblingsVar)
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
                  .each(Node::overlapping, "*searchNode", boundsVar).out("*childIds")
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
                  .each(Node::overlapping, "*searchNode", boundsVar).out("*ids")
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

  private static List<Object> restList(List<Object> l) {
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
        // .macro(rootNode("*rootNode"))
        .localSelect(rootPstate, Path.stay()).out("*rootNode")
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

  private <T> Block buildModTable(
    final String microbatchVar,
    final RTreeConvertorFunction<T> dataConvertor,
    final String modTableVar) {
    return Block
        .each(Ops.LOG_DEBUG, LOGGER, "buildModTable")
        .macro(rootNode("*rootNode"))
        .macro(explode(microbatchVar, "*data"))
        .each(Ops.PRINTLN, "DATA", "*data")
        .each((T data, OutputCollector collector) -> {
            RTreeCollector c = new RTreeCollector(collector);
            dataConvertor.invoke(data, c);
          },
          "*data").out("*modification")

        .each(Ops.PRINTLN, "Modification", "*modification")
        .macro(extractJavaFields("*modification", "*bounds", "*objectId"))
        // Find the node where this would be located
        // TODO parallel descent?
        .macro(chooseLeaf("*rootNode", "*bounds", "*chosenNode"))
        .macro(RamaAssert.assertMacro((Node v) -> {return v != null; },
                                      "*chosenNode"))
        // NOTE assumes chooseLeaf emits on *node's partition
        .directPartition("*taskId")
        .compoundAgg(
          CompoundAgg.map(
            new Expr(Node::nodeId, "*chosenNode"),
            Agg.list("*modification"))).out(modTableVar);
  }

  private Block hasNoMoreModesPred(final String modTableVar,
                                   final String noMoreOpsVar) {
    return Block
        .batchBlock(
          Block
          .allPartition()
          .localSelect(modTableVar, Path.stay()).out("*elems")
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
        .each(Ops.IDENTITY, new Expr(Ops.EQUAL, 0, "*maxSize"))
        .out(noMoreOpsVar);
  }

  private Block lookupNode(final String rootTaskIdVar,
                           final String nodeIdVar,
                           final String nodeVar) {
    final String tmpNodeVar = Helpers.genVar("node");
    return Block
        .macro(readNode(nodeIdVar, tmpNodeVar))
        .ifTrue(
          new Expr(Ops.IS_NULL, tmpNodeVar),
          Block.directPartition(rootTaskIdVar).macro(rootNode(nodeVar)),
          Block.each(Ops.IDENTITY, tmpNodeVar).out(nodeVar))
        .each(Ops.LOG_DEBUG, LOGGER,
              new Expr(Ops.TO_STRING,
                       "lookup nodeId: ", nodeIdVar, ", node: ", nodeVar))
        .macro(
          RamaAssert.assertMacro(
            Ops.EQUAL,
            nodeIdVar,
            new Expr(Node::nodeId, nodeVar)));
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

  public <T> Block handleModifications(
    final String microbatchVar,
    final RTreeConvertorFunction<T> dataConvertor) {

    return Block
        .each(Ops.LOG_DEBUG, LOGGER, "handleModifications")
        .each(Ops.CURRENT_TASK_ID).out("*taskId")
        .batchBlock(Block.macro(buildModTable(microbatchVar,
                                              dataConvertor,
                                              "$$modTable")))

        // Perform the insertion, looping to insert changes into parent nodes
        .loop(
          Block
          .yieldIfOvertime()
          .each(Ops.LOG_DEBUG, LOGGER, "handleModifications loop body start")
          .macro(hasNoMoreModesPred("$$modTable", "*hasNoMoreOps"))
          .ifTrue(
            "*hasNoMoreOps",
            // Nothing left to do, all modifications handled.
            Block
            .each(Ops.LOG_DEBUG, LOGGER, "Operations loop complete, emitting")
            .emitLoop(),
            // Perform changes to next node
            Block
            .batchBlock(
              Block
              .localSelect("$$modTable", Path.first()).out("*nodeOps")
              .each(Ops.LOG_DEBUG,
                    LOGGER,
                    new Expr(Ops.TO_STRING, "nodeOps: ", "*nodeOps"))
              .each(Ops.FIRST, "*nodeOps").out("*opNodeId")
              .each(Ops.LAST, "*nodeOps").out("*nodeOpsList")
              .macro(lookupNode("*taskId", "*opNodeId", "*currentNode"))
              .each(Ops.LOG_DEBUG,
                    LOGGER,
                    new Expr(Ops.TO_STRING, "nodeOpsList: ", "nodeOpsList"))
              .macro(updateNode(M,
                                idGenerator,
                                "*currentNode",
                                "*nodeOpsList",
                                "*newSiblings"))
              .each(Ops.LOG_DEBUG,
                    LOGGER,
                    new Expr(Ops.TO_STRING,
                             "UpdateNodes new siblings: ", "*newSiblings"))
              .each(List<Object>::size,"*newSiblings").out("*numSiblings")
              .each(Node::isRoot, "*currentNode").out("*isRoot")
              .ifTrue(new Expr(Ops.EQUAL, 0, "*numSiblings"),
                      // No splits creating new siblings
                      Block
                      .each(Ops.LOG_DEBUG, LOGGER, "no new siblings")
                      .macro(outputNode("*taskId", "*currentNode"))
                      .each(Ops.IDENTITY, null).out("*newOp")
                      .each(Node::parentId, "*currentNode").out("*parentId")
                      .each(Ops.IDENTITY, "*currentNode").out("*parent"),
                      // Have splits creating new siblings
                      Block
                      .ifTrue(new Expr(Ops.IDENTITY, "*isRoot"),
                              // the original node was root - we need a new
                              // root node.
                              Block
                              .each(Ops.LOG_DEBUG, LOGGER, "node was root")
                              .each(Node::nodeId, "*currentNode").out("*nodeId")
                              .each(Node::bounds, "*currentNode").out("*nodeBounds")
                              .macro(idGenerator.genId("*parentId"))
                              .each(RTree::createRootNode,
                                    "*parentId").out("*parent")
                              .each(Node::add,
                                    "*parent",
                                    "*nodeBounds",
                                    "*nodeId").out("*fred")
                              .each(Ops.LOG_DEBUG, LOGGER,
                                    "new root created")
                              .each(Ops.LOG_DEBUG, LOGGER,
                                    new Expr(Ops.TO_STRING,
                                             "new root created, taskId: ",
                                             "*taskId"))
                              .directPartition("*taskId")
                              .each(Ops.LOG_DEBUG, LOGGER,
                                    new Expr(Ops.TO_STRING,
                                             "Write new root: ", "*parent"))
                              .directPartition("*taskId")
                              .macro(writeRoot("*parent"))
                              .each(Node::setParentId,
                                    "*currentNode", "*parentId")
                              .macro(writeNode("*nodeId","*currentNode")),

                              // the original node was not root
                              Block
                              .each(Node::nodeId, "*currentNode").out("*nodeId")
                              .macro(writeNode("*nodeId", "*currentNode"))
                              .each(Node::parentId, "*currentNode").out("*parentId")
                              .each(Ops.IDENTITY,"*currentNode").out("*parent")))
              .each(Ops.LOG_DEBUG,
                    LOGGER,
                    new Expr(Ops.TO_STRING, "parent node: ", "*parent"))

              .each(Node::nodeId, "*currentNode").out("*nodeId")
              .macro(writeNode("*nodeId", "*currentNode"))
              .macro(saveAndOps("*newSiblings", "*newOps"))
              .directPartition("*taskId")
              .macro(updateModTable("$$modTable",
                                    "*opNodeId",
                                    "*parentId",
                                    "*newOps"))
              .each(Ops.LOG_DEBUG,
                    LOGGER,
                    new Expr(Ops.TO_STRING,
                             "End of loop body",
                             "  opNodeId: ", "*opNodeId",
                             ", Parent Id: ", "*parentId",
                             ", newOps: ", "*newOps")))
            .continueLoop()))
        .each(Ops.LOG_DEBUG, LOGGER,  "Loop complete")

        .batchBlock(Block.macro(propagateRootNode()));
  }

  private Block saveAndOps(final String newSiblingsVar,
                           final String newOpsVar) {
    return Block
        .loopWithVars(
                LoopVars
                .var("*siblings", newSiblingsVar)
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
                                 "Saving sibling: ", "*newSibling"))
                  .macro(writeNode("*newId", "*newSibling"))
                  .each(RTreeCollector.AddObject::mkAddObject,
                        "*newBounds",
                        "*newId").out("*newOp")
                  .each(Ops.LOG_ERROR, LOGGER,
                        new Expr(Ops.TO_STRING,
                                 "Ops: ", "*ops", " ",
                                 new Expr(Ops.CLASS, "*ops")))
                  .each(Ops.LOG_ERROR, LOGGER,
                        new Expr(Ops.TO_STRING, "newOp: ", "*newOp"))
                  .each(RTree::<RTreeCollector.AddObject>conjList,
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
    final String newOpsVar) {
    return Block
        .ifTrue(
          new Expr(RTree::isEmptyList, newOpsVar),
          Block.localTransform(
            modTableVar,
            // remove what we just processed
            Path.key(opNodeIdVar).termVoid()),
          Block.localTransform(
            modTableVar,
            Path
            .multiPath(
              // remove what we just processed
              Path.key(opNodeIdVar).termVoid(),
              // add new sibling nodes
              Path
              .key(parentIdVar)
              .nullToVal(new Expr(Vector::empty))
              .term(Vector::into, newOpsVar))));
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
  }


  /** Declare all the pobjects required for the RTree. */
  public void declare(final Topologies topologies,
                      final MicrobatchTopology topology) {
    declarePStates(topology);
    declareQueries(topologies);
  }

}
