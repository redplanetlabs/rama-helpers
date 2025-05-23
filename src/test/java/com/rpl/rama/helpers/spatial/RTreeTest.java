package com.rpl.rama.helpers.spatial;

import com.rpl.rama.AckLevel;
import com.rpl.rama.Agg;
import com.rpl.rama.Block;
import com.rpl.rama.Depot;
import com.rpl.rama.Expr;
import com.rpl.rama.Path;
import com.rpl.rama.PState;
import com.rpl.rama.QueryTopologyClient;
import com.rpl.rama.RamaModule;
import com.rpl.rama.helpers.ModuleUniqueIdPState;
import static com.rpl.rama.helpers.TopologyUtils.extractJavaFields;

import com.rpl.rama.module.MicrobatchTopology;
import com.rpl.rama.object.DepotPartitionInfo;
import com.rpl.rama.ops.Ops;
import com.rpl.rama.test.InProcessCluster;
import com.rpl.rama.test.LaunchConfig;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import org.junit.Test;

// import org.apache.logging.log4j.LogManager;
// import org.apache.logging.log4j.Logger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import clojure.lang.IFn.LOOD;

public class RTreeTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(RTreeTest.class);
  // private static final Logger LOGGER = LogManager.getLogger(RTreeTest.class);

  /** A module implementing a 2D spacial index of String values */
  public static class Module implements RamaModule {
    ModuleUniqueIdPState idGenerator = new ModuleUniqueIdPState("$$objectId");

    @Override
    public void define(Setup setup, Topologies topologies) {
      setup.declareDepot("*depot", Depot.random());

      MicrobatchTopology m = topologies.microbatch("m");
      m.pstate("$$object", PState.mapSchema(Long.class, Object.class));

      // This is just a test convenience
      m.pstate("$$objectLookup", PState.mapSchema(Object.class, Long.class));

      idGenerator.declarePState(m);

      // declare the RTree
      final int dimensions = 2;
      final int branchingFactor = 2;
      final int minChildren = 1;
      RTree rTree = new RTree(dimensions,
                              branchingFactor,
                              minChildren,
                              "test");
      rTree.declare(topologies, m);

      // ETL
      m.source("*depot").out("*microbatch")
          .batchBlock(
            Block
            .each(Ops.LOG_ERROR, LOGGER, "New Microbatch")
            .explodeMicrobatch("*microbatch").out("*v")
            .macro(idGenerator.genId("*objectId"))
            .macro(extractJavaFields("*v", "*bounds", "*object"))
            .each(Ops.LOG_ERROR,
                  LOGGER,
                  new Expr(Ops.TO_STRING,
                           "objectId=", "*objectId",
                           ", MB Process: ", "*v"))
            .hashPartition("$$object", "*objectId")
            .localTransform("$$object",
                            Path.key("*objectId").termVal("*object"))

            .hashPartition("$$objectLookup", "*object")
            .localTransform("$$objectLookup",
                            Path.key("*object").termVal("*objectId"))
            .each(Ops.PRINTLN,
                  "Added object",
                  "*objectId",
                  "*object",
                  "*bounds")
            .globalPartition()
            .agg(Agg.list(new Expr(Ops.TUPLE,
                                   "*bounds",
                                   "*objectId"))).out("$$objects"))
            .macro(
              rTree.handleModifications(
                "$$objects",
                (List<Object> data, RTreeCollector collector) -> {
                  collector.addObject(
                    (MBR)data.get(0),
                    (Long)data.get(1));}));
    }

  }


  @Test
  public void basicRootNodeSplitTest() throws Exception {
    LOGGER.error("basicRootNodeSplitTest");

    try(InProcessCluster cluster = InProcessCluster.create()) {
      final RamaModule module = new Module();
      cluster.launchModule(module, new LaunchConfig(1, 1));

      final Depot depot = cluster.clusterDepot(Module.class.getName(), "*depot");
      final PState nodes = cluster.clusterPState(Module.class.getName(), "$$test__nodes");
      final PState root = cluster.clusterPState(Module.class.getName(), "$$test__root");
      final PState object = cluster.clusterPState(Module.class.getName(), "$$object");
      final QueryTopologyClient q = cluster.clusterQuery(Module.class.getName(), "objectsInBounds");

      final double[] origin = {0, 0};
      final double[] ones = {1, 1};
      final double[] twos = {2, 2};
      final double[] oneHundreds = {100, 100};
      final double[] twoHundreds = {200, 200};

      final MBR oneBounds = new MBR(origin, ones);
      final MBR twoBounds = new MBR(origin, twos);
      final MBR twoHundredBounds = new MBR(oneHundreds, twoHundreds);
      final MBR allBounds = new MBR(origin, twoHundreds);

      System.out.println("START");

      // Test that we can append an object in the root node and query for it.
      depot.append(new AddObject(oneBounds, "a"), AckLevel.ACK);
      System.out.println("Appended one entry");
      cluster.waitForMicrobatchProcessedCount(module.getClass().getName(),
                                              "m",
                                              1);
      System.out.println("Processed one entry in root node");
      {
        DepotPartitionInfo dpi = depot.getPartitionInfo(0);
        assertEquals(1, dpi.getEndOffset());

        String a = object.selectOne(Path.key(0L));
        assertNotNull("An object has been recorded", a);
        assertEquals("Object has been recorded correctly", "a", a);

        INode node = root.selectOne(Path.stay());
        assertNotNull(node);
        assertTrue(node.isLeaf());
        assertTrue(node instanceof LeafNode);
        assertEquals(1, ((Node)node).count());

        assertEquals(new ArrayList<>(Arrays.asList(0L)),
                     new ArrayList<>((List<Long>)q.invoke(oneBounds)));
        assertEquals(new ArrayList<>(Arrays.asList(0L)),
                     new ArrayList<>((List<Long>)q.invoke(twoBounds)));
        assertEquals(new ArrayList<>(Arrays.asList()),
                     new ArrayList<>((List<Long>)q.invoke(twoHundredBounds)));
      }


      // Append another object in the root node and query for it.
      /* depot.append(new AddObject(oneBounds, "a"), AckLevel.ACK); */
      System.out.println("Appended second entry");
      depot.append(new AddObject(twoBounds, "b"), AckLevel.ACK);
      cluster.waitForMicrobatchProcessedCount(module.getClass().getName(),
                                              "m",
                                              2);
      System.out.println("Processed second entry in root node");
      {
        DepotPartitionInfo dpi = depot.getPartitionInfo(0);
        assertEquals(2, dpi.getEndOffset());

        Node node = root.selectOne(Path.stay());
        assertTrue(node instanceof Node);
        assertTrue(node.isLeaf());
        assertEquals(2, node.count());

        assertEquals(new ArrayList<>(Arrays.asList(0L, 1L)),
                     new ArrayList<>((List<Long>)q.invoke(oneBounds)));
        assertEquals(new ArrayList<>(Arrays.asList(0L, 1L)),
                     new ArrayList<>((List<Long>)q.invoke(twoBounds)));
        assertEquals(new ArrayList<>(Arrays.asList()),
                     new ArrayList<>((List<Long>)q.invoke(twoHundredBounds)));
      }

      System.out.println("Appended third entry");
      // Append another object when the root node is full and query for it.
      depot.append(new AddObject(twoHundredBounds, "c"), AckLevel.ACK);
      cluster.waitForMicrobatchProcessedCount(module.getClass().getName(),
                                              "m",
                                              3);
      System.out.println("Processed third entry, splitting root node");
      {
        DepotPartitionInfo dpi = depot.getPartitionInfo(0);
        assertEquals(3, dpi.getEndOffset());

        final Node rootNode = root.selectOne(Path.stay());
        final Node childNode0 = nodes.selectOne(Path.key(0L));
        final Node childNode1 = nodes.selectOne(Path.key(1L));

        System.out.println("Root node after processing " + rootNode);
        System.out.println("Node 0 after processing " + childNode0);
        System.out.println("Node 1 after processing " + childNode1);

        assertTrue(rootNode instanceof NonLeafNode);
        assertFalse(rootNode.isLeaf());
        assertEquals(2, rootNode.nodeId());
        assertEquals(2, rootNode.parentId());
        assertEquals(2, rootNode.count());
        {
          final Object[] children
            = ((Collection<Child>)rootNode.children)
            .stream().map(Child::childId).toArray();
          assertArrayEquals(new Object[] { 0L, 1L }, children);
        }
        assertEquals(allBounds, rootNode.bounds());

        assertEquals(0, childNode0.nodeId());
        assertEquals(2, childNode0.parentId());
        assertTrue(childNode0.isLeaf());
        assertEquals(2, childNode0.count());
        {
          final Object[] children
            = ((Collection<Child>)childNode0.children)
            .stream().map(Child::childId).toArray();
          assertArrayEquals(new Object[] { 0L, 1L }, children);
        }
        assertEquals(twoBounds, childNode0.bounds());

        assertEquals(1, childNode1.nodeId());
        assertEquals(2, childNode1.parentId());
        assertTrue(childNode0.isLeaf());
        assertEquals(1, childNode1.count()); // object 2
        {
          final Object[] children
            = ((Collection<Child>)childNode1.children)
            .stream().map(Child::childId).toArray();
          assertArrayEquals(new Object[] { 2L }, children);
        }
        assertEquals(twoHundredBounds, childNode1.bounds());

        assertEquals(new ArrayList<>(Arrays.asList(0L, 1L)),
                     new ArrayList<>((List<Long>)q.invoke(oneBounds)));
        assertEquals(new ArrayList<>(Arrays.asList(0L, 1L)),
                     new ArrayList<>((List<Long>)q.invoke(twoBounds)));
        assertEquals(new ArrayList<>(Arrays.asList(2l)),
                     new ArrayList<>((List<Long>)q.invoke(twoHundredBounds)));
      }
    }
    LOGGER.error("basicRootNodeSplitTest done");
  }

  @Test
  public void multiRootNodeSplitTest() throws Exception {
    LOGGER.error("multiRootNodeSplitTest");

    try(InProcessCluster cluster = InProcessCluster.create()) {
      final RamaModule module = new Module();
      cluster.launchModule(module, new LaunchConfig(2, 2));

      final Depot depot = cluster.clusterDepot(Module.class.getName(), "*depot");
      final PState nodes = cluster.clusterPState(Module.class.getName(), "$$test__nodes");
      final PState root = cluster.clusterPState(Module.class.getName(), "$$test__root");
      final PState object = cluster.clusterPState(Module.class.getName(), "$$object");
      final QueryTopologyClient q = cluster.clusterQuery(Module.class.getName(), "objectsInBounds");
      final QueryTopologyClient<List<Long>> dump
        = cluster.clusterQuery(Module.class.getName(), "dumpTree");
      final QueryTopologyClient<List<String>> dumpDot
        = cluster.clusterQuery(Module.class.getName(), "dumpDot");
      final QueryTopologyClient<Boolean> verify
        = cluster.clusterQuery(Module.class.getName(), "verifyTree");

      final double[] origin = {0, 0};
      final double[] ones = {1, 1};
      final double[] twos = {2, 2};
      final double[] threes = {3, 3};
      final double[] fours = {4, 4};
      final double[] fives = {5, 5};
      final double[] oneHundreds = {100, 100};
      final double[] twoHundreds = {200, 200};

      final MBR oneBounds = new MBR(origin, ones);
      final MBR twoBounds = new MBR(origin, twos);
      final MBR threeBounds = new MBR(origin, threes);
      final MBR fourBounds = new MBR(origin, fours);
      final MBR fiveBounds = new MBR(origin, fives);

      final MBR twoHundredBounds = new MBR(oneHundreds, twoHundreds);
      final MBR allBounds = new MBR(origin, twoHundreds);

      System.out.println("START");

      // Test that we can append an object in the root node and query for it.
      depot.append(new AddObject(oneBounds, "a"), AckLevel.NONE);
      depot.append(new AddObject(twoBounds, "b"), AckLevel.NONE);
      depot.append(new AddObject(threeBounds, "c"), AckLevel.NONE);
      depot.append(new AddObject(fourBounds, "d"), AckLevel.NONE);
      depot.append(new AddObject(fiveBounds, "e"), AckLevel.NONE);

      System.out.println("Appended one entry");
      cluster.waitForMicrobatchProcessedCount(module.getClass().getName(),
                                              "m",
                                              5);
      DepotPartitionInfo dpi0 = depot.getPartitionInfo(0);
      DepotPartitionInfo dpi1 = depot.getPartitionInfo(1);
      assertEquals(5, dpi0.getEndOffset() + dpi1.getEndOffset());

      System.out.println("Processed five entries creating two levels");
      {
        /* INode node = root.selectOne(Path.stay()); */
        /* assertNotNull(node); */
        /* assertFalse(node.isLeaf()); */
        /* assertTrue(node instanceof NonLeafNode); */
        /* assertEquals(2, ((Node)node).count()); */

        LOGGER.error("Multi Dump");
        dump.invoke();
        List<String> elements = dumpDot.invoke();
        System.out.println("digraph G {");
        for (String s : elements) {
          System.out.println(s);
        }
        System.out.println("}");

        LOGGER.error("Verify " + verify.invoke());

        assertEquals(new ArrayList<>(Arrays.asList(4L, 3L, 0L, 2L, 1L)),
                     new ArrayList<>((List<Long>)q.invoke(oneBounds)));
        assertEquals(new ArrayList<>(Arrays.asList(4L, 3L, 0L, 2L, 1L)),
                     new ArrayList<>((List<Long>)q.invoke(twoBounds)));
        assertEquals(new ArrayList<>(Arrays.asList()),
                     new ArrayList<>((List<Long>)q.invoke(twoHundredBounds)));
      }

    }
    LOGGER.error("multiRootNodeSplitTest done");
  }

  public static class RandomObject {
    final public MBR bounds;
    final public long id;

    public RandomObject(MBR bounds, long id) {
      this.bounds = bounds;
      this.id = id;
    }

    @Override
    public String toString() {
      return "RandomObject [bounds=" + bounds + ", id=" + id + "]";
    }
  }

  private static List<RandomObject> generateObjects(
    final Random random,
    final MBR bounds,
    final int numObjects) {
    List<RandomObject> objects = new ArrayList<RandomObject>();
    for (long i = 0; i < numObjects; i++) {
      boolean isIntersect = random.nextBoolean();
      if (isIntersect && !objects.isEmpty()) {
        int elementIndex = random.nextInt(objects.size());
        RandomObject element = objects.get(elementIndex);
        MBR objectBounds = element.bounds.randomSubBounds(random);
        objects.add(new RandomObject(objectBounds, i));
      } else {
        MBR objectBounds = bounds.randomSubBounds(random);
        objects.add(new RandomObject(objectBounds, i));
      }
    }
    return objects;
  }

  @Test
  public void uncoordinatedTest() throws Exception {
    LOGGER.debug("uncoordinatedTest");
    long seed = new Random().nextLong();
    LOGGER.debug("uncoordinatedTest seed: " + seed);
    Random random = new Random(seed);

    final MBR bounds = new MBR(new double[]{0,0}, new double[]{100,1000});
    final int numObjects = 20;
    List<RandomObject> objects = generateObjects(random, bounds, numObjects);

    try(InProcessCluster cluster = InProcessCluster.create()) {
      final RamaModule module = new Module();
      cluster.launchModule(module, new LaunchConfig(4, 3));

      final Depot depot = cluster.clusterDepot(Module.class.getName(), "*depot");
      final PState nodes = cluster.clusterPState(Module.class.getName(), "$$test__nodes");
      final PState root = cluster.clusterPState(Module.class.getName(), "$$test__root");
      final PState object = cluster.clusterPState(Module.class.getName(), "$$object");
      final PState objectLookup = cluster.clusterPState(Module.class.getName(), "$$objectLookup");
      final QueryTopologyClient q = cluster.clusterQuery(Module.class.getName(), "objectsInBounds");
      final QueryTopologyClient<Boolean> verify
        = cluster.clusterQuery(Module.class.getName(), "verifyTree");
      final QueryTopologyClient<List<String>> dumpDot
        = cluster.clusterQuery(Module.class.getName(), "dumpDot");
      final QueryTopologyClient<List<Long>> dump
        = cluster.clusterQuery(Module.class.getName(), "dumpTree");
      final QueryTopologyClient<List<List<Object>>> dumpBounds
        = cluster.clusterQuery(Module.class.getName(), "dumpBounds");

      LOGGER.debug("START");

      // Test that we can append an object in the root node and query for it.
      for (int i = 0; i < numObjects ; i++) {
          RandomObject robject = objects.get(i);
          depot.append(new AddObject(robject.bounds, robject.id), AckLevel.ACK);
        }

      LOGGER.debug("Appended one entry");
      cluster.waitForMicrobatchProcessedCount(module.getClass().getName(),
                                              "m",
                                              numObjects);
      LOGGER.debug("Processed entries");

      LOGGER.error("Dump");
      dump.invoke();

      List<String> elements = dumpDot.invoke();
      System.out.println("digraph G {");
      for (String s : elements) {
        System.out.println(s);
      }
      System.out.println("}");

      List<List<Object>> boundsList = dumpBounds.invoke();
      RTreeHelpers.dumpBoundsList(boundsList);

      LOGGER.error("Verify " + verify.invoke());

      LOGGER.error("Checking objects");

      ArrayList<Long> objectIds = new ArrayList<>();

      for (int i = 0; i < numObjects ; i++) {
        LOGGER.error("XX "+ i + " " + objectLookup.selectOne(Path.key(Long.valueOf(i))));
        objectIds.add(objectLookup.selectOne(Path.key(Long.valueOf(i))));
      }

      for (int i = 0; i < numObjects ; i++) {
          RandomObject robject = objects.get(i);
          ArrayList<Long> foundObjects
            = new ArrayList<>((List<Long>) q.invoke(robject.bounds));

          System.out.println("Found "+foundObjects+
                             " for " + robject +
                             " i=" + i +
                             " objectId=" + objectIds.get(i));
          assertTrue(foundObjects.contains(objectIds.get(i)));
        }
    }

    LOGGER.debug("uncoordinatedTest done");
  }

}
