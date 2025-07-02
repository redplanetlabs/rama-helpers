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

public class Grid implements RamaSerializable {
  private static final Logger LOGGER = LoggerFactory.getLogger(Grid.class);

  public static class Node implements RamaSerializable {
    PersistentVector children;

    public Node() {
      this.children = Vector.empty();
    }

    public Node add(MBR bounds, long id) {
      children = children.cons(new Child(bounds, id));
      return this;
    }

    public Node addChild(Child child) {
      children = children.cons(child);
      return this;
    }

    public Node performOps(List<ModificationCollector.AddObject> ops) {
      for (ModificationCollector.AddObject op : ops) {
        add(op.bounds, op.objectId);
      }
      return this;
    }
  }

  /** The overall bounds of the grid. */
  public MBR bounds;

  /** The number of extents for each dimension of bounds. */
  public int[] numExtents;

  /** The total number of regions in the grid. */
  private long numRegions;

  private final String nodesPstate;

  public Grid(MBR bounds, int[] numExtents, final String gridName) {
    assert bounds.dimensions() == numExtents.length;
    this.bounds = bounds;
    this.numExtents = numExtents;
    this.numRegions = totalRegions(numExtents);

    this.nodesPstate = "$$" + gridName + "__nodes";
  }

  public static long totalRegions(int[] numExtents) {
    assert numExtents != null;
    assert numExtents.length > 0;

    long product = 1;
    for (int extent : numExtents) {
        product *= extent;
    }
    return product;
  }

  /** Return the index for the center-point of the given bounds. */
  private long boundsIndex(final MBR bounds) {
    long factor = 1;
    long result = 0;
    for (int dim = 0; dim < bounds.dimensions(); dim = dim + 1) {
      long i = Math.floorDiv(
        (long)(bounds.getCenterPoint(dim) - this.bounds.getMin(dim)),
        (long) bounds.getExtent(dim));
      result = result + i * factor;
      factor = factor * this.numExtents[dim];
    }
    return result;
  }

  /** Return the partition for the center-point of the given bounds. */
  private static long boundsPartition(final long boundsIndex,
                                      final long numPartitions) {
    return boundsIndex % numPartitions;
  }

  void declarePStates(final MicrobatchTopology topology) {
  }

  void declareQueries(final Topologies topology) {
  }

  /** Declare all the pobjects required for the Grid. */
  public void declare(final Topologies topologies,
                      final MicrobatchTopology topology) {
    declarePStates(topology);
    declareQueries(topologies);
  }

  private <T> Block buildModTable(
    final String userModTableVar,
    final ModificationConvertorFunction<T> dataConvertor,
    final String modTableVar,
    final String rootUpdateVar) {
    return Block
        .each(Ops.LOG_TRACE, LOGGER, "buildModTable")
        .each(Ops.MODULE_INSTANCE_INFO).out("*mii")
        .each(ModuleInstanceInfo::getNumTasks, "*mii").out("*numPartitions")
        .allPartition()
        .localSelect(modTableVar, Path.all()).out("*data")
        // .each(Ops.LOG_TRACE, LOGGER,
        //       new Expr(Ops.TO_STRING, "DATA ", "*data"))
        .each((T data, OutputCollector collector) -> {
            ModificationCollector c = new ModificationCollector(collector);
            dataConvertor.invoke(data, c);
          },
          "*data").out("*modification")

        .each(Ops.LOG_TRACE, LOGGER, "Modification")
        // .each(Ops.LOG_TRACE, LOGGER,
        //       new Expr(Ops.TO_STRING, "Modification ", "*modification"))

        // TODO extractJavaFields is not very efficient
        .macro(extractJavaFields("*modification", "*bounds", "*objectId"))
        .each(Grid::boundsIndex, this, "*bounds").out("*index")
        .each(Grid::boundsPartition,
              "*index",
              "*numPartitions").out("*partition")
        .directPartition("*partition")
        .localTransform(
          modTableVar,
          Path.key("*index").nullToList().afterElem().termVal("*modification"))
        ;
  }

  public <T> Block handleModifications(
    final String userModTableVar,
    final ModificationConvertorFunction<T> dataConvertor) {

    return Block
        .each(Ops.LOG_DEBUG, LOGGER, "handleModifications")
        .batchBlock(Block.keepTrue(false).materialize().out("$$rootUpdate"))
        .batchBlock(Block.keepTrue(false).materialize().out("$$modTable"))
        .batchBlock(Block.macro(buildModTable(userModTableVar,
                                              dataConvertor,
                                              "$$modTable",
                                              "$$rootUpdate")))
        .each(Ops.LOG_DEBUG, LOGGER, "buildModTable finished")

        .batchBlock(
          Block
          .allPartition()
          .each(Ops.LOG_TRACE, LOGGER, "Loop body for task")
          .localSelect("$$modTable", Path.all()).out("*nodeOps")
          .each(Ops.LOG_TRACE, LOGGER, "Loop body AA")
          // TODO move this destructuring into updateNode
          .each(Ops.FIRST, "*nodeOps").out("*index")
          .each(Ops.LAST, "*nodeOps").out("*nodeOpsList")
          .localSelect(nodesPstate, Path.key("*index")).out("*node")
          .macro(updateNode("*index", "*node", "*nodeOpsList"))
          .each(Ops.LOG_DEBUG, LOGGER, "handleModifications done"));
  }

  public Block updateNode(final String indexVar,
                          final String nodeVar,
                          final String opsVar) {
    return Block
        .each(Node::performOps, nodeVar, opsVar)
        .localTransform(nodesPstate, Path.key(indexVar).termVal(nodeVar));
  }
}
