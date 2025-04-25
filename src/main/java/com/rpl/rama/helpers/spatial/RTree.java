package com.rpl.rama.helpers.spatial;

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
import com.rpl.rama.module.MicrobatchTopology;
import com.rpl.rama.ops.Ops;
import com.rpl.rama.ops.OutputCollector;
import com.rpl.rama.ops.RamaFunction1;
import com.rpl.rama.ops.RamaFunction2;
import com.rpl.rama.ops.RamaFunction3;
import static com.rpl.rama.helpers.TopologyUtils.extractJavaFields;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import rpl.rama.java_api.integration__init;

public class RTree implements RamaSerializable {
  private final int dimensions;
  private final ModuleUniqueIdPState idGenerator;
  private final String objectsPstate;
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
  // private final Logger logger = LogManager.getLogger(RTree.class);

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
    this.objectsPstate = "$$" + treeName + "__objects";
  }

  private void declarePStates(final MicrobatchTopology topology) {
    idGenerator.declarePState(topology);
    topology.pstate(objectsPstate,
                    PState.mapSchema(Long.class, Object.class));
    topology.pstate(rootPstate,
                    Object.class).global();
    topology.pstate(nodesPstate,
                    PState.mapSchema(Long.class, Node.class));
  }

  public NonLeafNode createNonLeafNode(long id, Node child1, Node child2) {
    return (NonLeafNode) new NonLeafNode(id, id)
        .add(child1.bounds(), child1.nodeId())
        .add(child2.bounds(), child2.nodeId());
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
      .ifTrue(new Expr(Ops.EQUAL, isLeafVar, true),
              Block
              .each(Ops.IDENTITY, true).out(leafNodeIsRootVar)
              .each(Ops.IDENTITY, rootNodeVar).out(leafNodeVar),

    // CL3. [Choose subtree.] If Af is not a leaf, let F be the entry in N whose
    // rectangle F.I needs least enlargement to include E.I. Resolve ties by
    // choosing the entry with the rectangle of smallest area
              Block
              .each(Ops.IDENTITY, false).out(leafNodeIsRootVar)
              // TODO
              .each(Ops.IDENTITY, null).out(leafNodeVar));
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
  private Block splitNode(final String leafNodeVar, final String newNodeVar) {
    final String newNodeIdVar = Helpers.genVar("newNodeId");
    return Block
      .macro(idGenerator.genId(newNodeIdVar))
      .each(LeafNode::splitNode, leafNodeVar, newNodeIdVar, m).out(newNodeVar);
  }

  /** Adjust tree.
   * Persists newNodes into the tree.
   */
  private Block adjustTree(final String nodeVar,
                           final String newNodeVar,
                           /* final String nodeIsRootVar, */
                           /* final String needsNewRootVar, */
                           final String newParentVar) {
    final String parentIdVar = Helpers.genVar("parentId");
    final String parentVar = Helpers.genVar("parent"); // P
    final String nodeIdVar = Helpers.genVar("nodeId");
    final String isParentFullVar = Helpers.genVar("isParentFull");
    final String newParentsVar = Helpers.genVar("newParents");
    final String newNodeBoundsVar = Helpers.genVar("newNodeBounds");
    final String newNodeIdVar = Helpers.genVar("newNodeIdVar");
    final String newParentIdVar = Helpers.genVar("newParentId");
    final String nodeIsRootVar = Helpers.genVar("nodeIsRoot");
    // AT1. [Initialize.] Set N=L. If L was split previously, set NN to be the
    // resulting second node.
    return Block
      .each(Ops.PRINTLN, "adjustTree")
      // .each(Ops.LOG_ERROR, logger, "adjustTree")
      /* .each(Ops.EXPAND, newNodesVar).out(nodeVar, newNodeVar) */
      .each(Node::nodeId, nodeVar).out(nodeIdVar)
      // AT2. [Check if done.] If N is the root, stop.
      // .loopWithVars(LoopVars
      // 		    .var(nodeIdVar, nodeIdVar)
      // 		    .var(nodeVar, nodeVar)
      // 		    .var(newNodeVar, newNodeVar),
      // 		    Block
      // 	    .ifTrue(new Expr(Ops.EQUAL, nodeIsRootVar, true),
      // 		    Block
      // 		    .each(Ops.IDENTITY, nodeVar).out(outVar)
      // 		    .emitLoop(outVar),

      // Block

      // AT3. [Adjust covering rectangle in parent entry.]
      .each(Node::isRoot, nodeVar).out(nodeIsRootVar)
      // .each(Ops.LOG_ERROR, logger, "adjustTree newNode", newNodeVar)
      .each(Ops.PRINTLN, "adjustTree newNode", newNodeVar)
      .ifTrue(new Expr(Ops.AND,
                       new Expr(Ops.IS_NOT_NULL, newNodeVar),
                       new Expr(Ops.EQUAL, nodeIsRootVar, true)),
              Block
              .macro(idGenerator.genId(newParentIdVar))
              .each(RTree::createNonLeafNode,
                    this,
                    newParentIdVar,
                    nodeVar,
                    newNodeVar).out(newParentVar),

              // Let P be the parent node of N
              Block
              .each(Node::parentId, nodeVar).out(parentIdVar)
              .select(objectsPstate, Path.key(parentIdVar)).out(parentVar)
              // .each(RTree<T>::isFull, this, parentVar).out(isParentFullVar)

              // and let EN be N's entry in
              // P.  Adjust En.I so that it tightly encloses all entry
              // rectangles in N.
              .each(Node::updateChild, parentVar, nodeVar)

              // AT4. [Propagate node split upward.]
              // If N has a partner NN resulting from an earlier split,
              // create a new entry ENN with ENN.p pointing to NN and Em
              // .I enclosing all rectangles in NN. Add Enn to P if there
              // is room Otherwise, invoke SplitNode to produce P and PP
              // containing Em and all P’s old entries.
              // .each(Ops.IDENTITY, null).out(newNodeVar)
              .ifTrue(new Expr(Ops.IS_NOT_NULL, newNodeVar),
                      Block
                      .each(Node::bounds, newNodeVar).out(newNodeBoundsVar)
                      .each(Node::nodeId, newNodeVar).out(newNodeIdVar)
                      .macro(insertInNode(newNodeVar, newNodeBoundsVar, newNodeIdVar, newParentVar))
                      // Block
                      // .each(RTree<T>::isFull, this, parentVar).out(isParentFullVar)
                      // .ifTrue(new Expr(Ops.EQUAL, isParentFullVar, true),
                      // 	    Block
                      // 	    .macro(splitNode(parentVar, isParentFullVar, newParentsVar))
                      // 	    .each(Ops.EXPAND, newParentsVar).out(parentVar, newNodeVar)
                      // 	    .each(Node::nodeId, parentVar).out(parentIdVar))
                      )
              // AT5. [Move up to next level.] Set N=P and set NN-PP if a
              // split occurred. Repeat from AT2.
              // .ifTrue(new Expr(Ops.EQUAL, parentIdVar, nodeIdVar),
              // 	    Block.emitLoop(),
              // 	    Block.continueLoop(parentIdVar, parentVar, newNodeVar))))
              );
  }

  /** Insert entry or child into node.
   */
  private Block insertInNode(final String nodeVar,
                             final String boundsVar,
                             /* This can be object or child node id */
                             final String idVar,
                             /* this is the output, a new sibling node */
                             final String newSiblingNodeVar) {
    final String leafNodeIsRootVar = Helpers.genVar("leafNodeIsRoot");
    final String isFullVar = Helpers.genVar("isFull");
    final String nodeIdVar = Helpers.genVar("nodeId");

    return Block
      .each(Ops.PRINTLN, "insertInNode", nodeVar, boundsVar, idVar)

      // 12. [Add record to node.]
      .each(RTree::isFull, this, nodeVar).out(isFullVar)
      .each(Ops.PRINTLN, "insertInNode isFull", isFullVar)

      // If L doesn't has room for another entry
      .ifTrue(isFullVar,
              // invoke splitNode to obtain L and LL containing E and all the
              // old entries of L.
              Block
              .macro(splitNode(nodeVar, newSiblingNodeVar))
              ,
              // install E in L
              Block
              .each(Ops.IDENTITY, Arrays.asList()).out(newSiblingNodeVar))

      .each(Node::nodeId, nodeVar).out(nodeIdVar)
      // split could go async, so may need batch blocks and a depot for updates
      .each(Node::add, nodeVar, boundsVar, idVar)
      .each(Ops.PRINTLN, "insertInNode after add", nodeVar)
      .each(Node::isRoot, nodeVar).out(leafNodeIsRootVar)
      .ifTrue(leafNodeIsRootVar,
              Block.localTransform(rootPstate, Path.termVal(nodeVar)),
              Block.localTransform(nodesPstate,
                                   Path.key(nodeIdVar).termVal(nodeVar)))
      .each(Ops.PRINTLN, "insertInNode done");
  }

  /** Perform an insert, creating a new node if needed and updating the parent.
      If new node is created, insert the created node into the parent and
      repeat.
   */
  private Block insertLoop(final String nodeVar,
                           final String childBoundsVar,
                           /* This can be object or child node id */
                           final String childIdVar,
                           /* this is the output, the new parent node */
                           final String parentNodeVar) {
    final String newSiblingVar = Helpers.genVar("newSibling");
    final String newSiblingBoundsVar = Helpers.genVar("newSiblingBounds");
    final String newSiblingIdVar = Helpers.genVar("newSiblingId");

    return Block
      .loopWithVars(LoopVars
                    .var(nodeVar, nodeVar) // node to insert into
                    .var(childBoundsVar, childBoundsVar)
                    .var(childIdVar, childIdVar),
                    Block
                    .macro(insertInNode(nodeVar,
                                        childBoundsVar,
                                        childIdVar,
                                        newSiblingVar))
                    .each(Ops.PRINTLN,
                          "insertLoop, newSiblingVar",
                          newSiblingVar)

                    // 13. [Propagate changes upward.] Invoke AdjustTree on L,
                    // also passing LL if a split was performed.
                    .macro(adjustTree(nodeVar, newSiblingVar, parentNodeVar))

                    .ifTrue(new Expr(Ops.IS_NULL, newSiblingVar),
                            Block.emitLoop(nodeVar),
                            Block
                            .each(Node::nodeId, newSiblingVar).out(newSiblingIdVar)
                            .each(Node::bounds, newSiblingVar).out(newSiblingBoundsVar)
                            .continueLoop(parentNodeVar, newSiblingBoundsVar, newSiblingIdVar)))
      ;
  }

  private LeafNode constructRoot(long id) {
      return new LeafNode(id, id);
  }

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
              .localTransform(rootPstate, Path.termVal(rootNodeVar)),
              Block
              .each(Ops.PRINTLN, "root node already exists")
              .each(Ops.IDENTITY, currentRootNodeVar).out(rootNodeVar))
      .each(Ops.PRINTLN, "Root node", rootNodeVar);
  }

  /** Insert a new index entry E */
  private Block insert(final String boundsVar, final String objectVar) {
    final String leafNodeVar = Helpers.genVar("leafNode");
    final String leafNodeIsRootVar = Helpers.genVar("leafNodeIsRoot");
    final String idVar = Helpers.genVar("id");
    final String rootNodeVar = Helpers.genVar("rootNode");
    final String rootNodeIdVar = Helpers.genVar("rootNodeId");
    final String newRootNodeVar = Helpers.genVar("newRootNode");
    final String newRootNodeIdVar = Helpers.genVar("newRootNodeId");

    return
      // 11. [Find position for new record.]
      // Invoke ChooseLeaf to select a leaf node L in which to place E.
      Block
      // [Initialize.] Set N to be the root node.
      .macro(rootNode(rootNodeVar))
      .each(Node::nodeId, rootNodeVar).out(rootNodeIdVar)
      .macro(chooseLeaf(rootNodeVar, leafNodeVar, leafNodeIsRootVar))
      .macro(idGenerator.genId(idVar))
      .localTransform(objectsPstate, Path.key(idVar).termVal(objectVar))
      .macro(insertInNode(leafNodeVar, boundsVar, idVar, newRootNodeVar))
      // // 12. [Add record to leaf node.]
      // .each(RTree<T>::isFull, this, leafNodeVar).out(isFullVar)
      // // If L doesn't has room for another entry
      // .ifTrue(isFullVar,
      // 	      // invoke splitNode to obtain L and LL containing E and all the
      // 	      // old entries of L.
      // 	      Block
      // 	      .macro(splitNode(leafNodeVar, leafNodeIsRootVar, newNodesVar))
      // 	      .each(Ops.EXPLODE, newNodesVar).out(newNodeVar)
      // 	      .each(Node::nodeId, newNodeVar).out(newNodeIdVar)
      // 	      .localTransform(nodesPstate, Path.key(newNodeIdVar).termVal(newNodeVar))
      // 	      ,
      // 	      // install E in L
      // 	      Block
      // 	      .macro(nodeId.genId(idGeneratorVar))
      // 	      .each(Node::nodeId, leafNodeVar).out(nodeIdVar)
      // 	      .each(Node::add, leafNodeVar, boundsVar, idGeneratorVar).out(leafNodeVar)
      // 	      .each(Ops.IDENTITY, null).out(newNodeVar)
      // 	      .localTransform(objectsPstate, Path.key(idGeneratorVar).termVal(objectVar))
      // 	      .ifTrue(leafNodeIsRootVar,
      // 		      Block.localTransform(rootPstate, Path.termVal(leafNodeVar)),
      // 		      Block.localTransform(nodesPstate,
      // 					   Path.key(nodeIdVar).termVal(leafNodeVar)))
      // 	      .each(Ops.TUPLE, leafNodeVar).out(newNodesVar))

      // // 13. [Propagate changes upward.] Invoke AdjustTree on L, also passing LL
      // // if a split was performed.
      // .macro(adjustTree(newNodesVar, nodeIsRootVar, needsNewRootVar, rootNodeVar))

    // 14. [Grow tree taller.] If node split propagation caused the root to
    // split, create a new root whose children are the two resulting nodes.
      .each(Node::nodeId, newRootNodeVar).out(newRootNodeIdVar)
      .ifTrue(new Expr(Ops.NOT_EQUAL, rootNodeIdVar, newRootNodeIdVar),
              Block
              .localTransform(rootPstate, Path.stay().termVal(newRootNodeVar)));
  }

  /** Perform all operations from nodeOpsVar on nodeVar.

      newSiblingIdVar will contain any new sibling nodes that have been created.
   */
  protected static Block updateNode(
    final int branchingFactor,
    final ModuleUniqueIdPState idGenerator,
    final String nodeVar,
    final String nodeOpsVar,
    // outputs
    final String newSiblingsVar) {

    return Block
        .each(Ops.PRINTLN, "updateNode", nodeVar, nodeOpsVar)
        .loopWithVars(
          LoopVars
          .var("*currentNode", nodeVar)
          .var("*nodeOpsRemaining", nodeOpsVar)
          .var("*newSiblings", new ArrayList<>()),
          Block
          .ifTrue(
            new Expr(Ops.AND,
                     new Expr(Node::isFull, "*currentNode", branchingFactor),
                     new Expr(Ops.NOT,
                              new Expr(List<Object>::isEmpty,
                                       "*nodeOpsRemaining"))),
            Block
            .macro(idGenerator.genId("*newNodeId"))
            .each(Node::newSibling,
                  "*currentNode",
                  "*newNodeId").out("*newNode")
            .each((final List<Node> l, final Node node) -> {
                l.add(node);
                return l;
            },
              "*newSiblings",
              "*newNode")
            .continueLoop("*newNode",
                          "*nodeOpsRemaining",
                          "*newSiblings"),
            Block
            .each(List<Node>::size, "*nodeOpsRemaining").out("*opCount")
            .ifTrue(new Expr(Ops.EQUAL, "*opCount", 0),
                    Block.emitLoop("*newSiblings"),
                    Block
                    .each(List<Node>::get,
                          "*nodeOpsRemaining",
                          0).out("*nodeOp")
                    .macro(extractJavaFields("*nodeOp", "*bounds", "*objectId"))
                    .each(Node::add, "*currentNode", "*bounds", "*objectId")
                    .each((List<Node> l, Integer index) -> {
                        l.remove((int)index);
                        return l; },
                      "*nodeOpsRemaining",
                      0)
                    .continueLoop("*currentNode",
                                  "*nodeOpsRemaining",
                                  "*newSiblings"))))
        .out(newSiblingsVar);
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
      .each(Node::isLeaf, rootVar).out(isLeafVar)
      .each(Ops.PRINTLN, "search isLeaf", isLeafVar)
      .ifTrue(new Expr(Ops.EQUAL, isLeafVar, false),
      // S1. [Search subtrees.] If T is not a leaf, check each entry E to
      // determine whether E.I overlaps S. For all overlapping entries, invoke
      // Search on the tree whose root node is pointed to by E.p .
              Block.each(Ops.IDENTITY, "todo").out(outVar),
      // S2. [Search leaf node.] If T is a leaf, check all entries E to
      // determine whether E.I overlaps S. If so, E is a qualifying record.
              Block
              .each(Node::overlapping, rootVar, boundsVar).out(idGeneratorsVar)
              .each(Ops.EXPLODE, idGeneratorsVar).out(idGeneratorVar)
              .localSelect(objectsPstate, Path.key(idGeneratorVar)).out(outVar)
      );
  }

  public static void addObject(MBR bounds, Long objectId, OutputCollector collector) {
    collector.emit(new AddObject(bounds, objectId));
  }

  /** Functional interface for expected dataConverter signature */
  public interface RTreeConvertorFunction<T>  extends RamaSerializable {
    public void invoke(T data, RTreeCollector collector);
  }

  // public <T> Block handleModifications(final String microbatchVar,
  //                                      final RTreeConvertorFunction<T> dataConvertor) {
  //   final String dataVar = Helpers.genVar("*data");
  //   final String modificationVar = Helpers.genVar("*modification");
  //   return Block
  //     .batchBlock(Block
  //                 .explodeMicrobatch(microbatchVar).out(dataVar)
  //                 .each((T data, OutputCollector collector) -> {
  //                     RTreeCollector c = new RTreeCollector(collector);
  //                     dataConvertor.invoke(data, c);
  //                   },
  //                   dataVar).out(modificationVar)
  //                 .agg(Agg.list(modificationVar)).out("$$p"));}

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
    final String leafNodeVar = Helpers.genVar("leafNode");
    final String leafNodeIsRootVar = Helpers.genVar("leafNodeIsRoot");
    final String rootNodeVar = Helpers.genVar("rootNode");
    final String rootNodeIdVar = Helpers.genVar("rootNodeId");

    final String nodeOpsVar = Helpers.genVar("nodeOps");
    final String nodeVar = Helpers.genVar("node");
    final String opsVar = Helpers.genVar("ops");


    // TODO hash by node id to start with.
    // TODO don't need genVar's for names inside a single batch block
    return Block
        .batchBlock(
          Block
          .macro(rootNode(rootNodeVar))
          .macro(explode(microbatchVar, "*data"))
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
                            leafNodeVar,
                            leafNodeIsRootVar))
          .globalPartition()
          .compoundAgg(
            CompoundAgg.map(
              leafNodeVar,
              Agg.list("*modification"))).out(nodeChangeTableVar))

        // Perform the insertion, looping to insert changes into parent nodes
        .loop(
            Block
            .each(Ops.PRINTLN, "loop")
            .batchBlock(
              Block
              .allPartition()
              .localSelect(nodeChangeTableVar, Path.view(Ops.SIZE)).out("*size")
              .globalPartition()
              .agg(Agg.max("*size")).out("$$maxSize"))
            .localSelect("$$maxSize", Path.stay()).out("*maxSize")
            .each(Ops.PRINTLN, "maxSize", "*maxSize")
            .ifTrue(
              new Expr(Ops.EQUAL, 0, "*maxSize"),
              Block.emitLoop(),
              Block
              .batchBlock(
                Block
                .localSelect(nodeChangeTableVar, Path.all()).out(nodeOpsVar)

                .macro(updateNode(nodeVar,
                                  nodeOpsVar,
                                  "*newSiblings"))
                .each(Ops.PRINTLN,"UpdateNodes new siblings", "*newSiblings")
                .each(List<Object>::size,"*newSiblings").out("*numSiblings")
                .ifTrue(new Expr(Ops.EQUAL, 0, "*numSiblings"),
                        Block
                        .each(Ops.PRINTLN,"no new siblings")
                        .each(Node::isRoot, nodeVar).out("*isRoot")
                        .ifTrue(new Expr(Ops.IDENTITY, "*isRoot"),
                                Block.localTransform(rootPstate,
                                                     Path.termVal(nodeVar)))
                        .each(Ops.IDENTITY, null).out("*newOp"))
                .each(Ops.EXPLODE, "*newSiblings").out("*newSibling")
                .each(Node::bounds, "*newSibling").out("*newBounds")
                .each(Node::nodeId, "*newSibling").out("*newId")
                .each(RTreeCollector.AddObject::mkAddObject,
                              "*newBounds",
                              "*newId").out("*newOp")
                .each(Node::parentId, nodeVar).out("*parentNodeId")
                .each(Ops.PRINTLN,"parent node id", "*parentNodeId")
                .globalPartition()
                .compoundAgg(
                  CompoundAgg.map(
                    "*parentNodeId",
                    Agg.list("*newOp"))).out("*newChangeTable")
                .localTransform(
                  nodeChangeTableVar,
                  Path.termVal("*newChangeTable"))
                .each(Ops.PRINTLN,"End of loop body"))
              .continueLoop()))

        // Broadcast root node to all tasks
        .batchBlock(
          Block
          .each(Ops.PRINTLN, "Updating global partitions")
          .globalPartition()
          .macro(rootNode("*rootNode"))
          .allPartition()
          .localTransform(rootPstate, Path.termVal("*rootNode")))
        ;}

  private void declareQueries(final Topologies topologies) {
    topologies.query("objectsInBounds", "*bounds").out("*objects")
        .each(Ops.PRINTLN, "objectsInBounds", "*bounds")
        .localSelect(rootPstate, Path.stay()).out("*root")
        .macro(search("*bounds", "*root", "*objects"))
        .originPartition()
        .agg(Agg.list("*objects")).out("*objects");
  }

  /** Declare all the pobjects required for the RTree. */
  public void declare(final Topologies topologies,
                      final MicrobatchTopology topology) {
    declarePStates(topology);
    declareQueries(topologies);
  }
}
