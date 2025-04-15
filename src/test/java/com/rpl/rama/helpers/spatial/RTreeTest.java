package com.rpl.rama.helpers.spatial;

import com.rpl.rama.AckLevel;
import com.rpl.rama.Depot;
import com.rpl.rama.Path;
import com.rpl.rama.PState;
import com.rpl.rama.QueryTopologyClient;
import com.rpl.rama.RamaModule;
import com.rpl.rama.RamaSerializable;
import com.rpl.rama.helpers.TopologyUtils.ExtractJavaField;
import static com.rpl.rama.helpers.TopologyUtils.extractJavaFields;
import com.rpl.rama.module.StreamTopology;
import com.rpl.rama.object.DepotPartitionInfo;
import com.rpl.rama.test.InProcessCluster;
import com.rpl.rama.test.LaunchConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Test;

public class RTreeTest {

  /** Depot value */
  public static class AddObject implements RamaSerializable {
    public final MBR bounds;
    public final Object value;

    public AddObject(final MBR bounds, Object value) {
      this.bounds = bounds;
      this.value = value;
    }
  }

  /** A module implementing a 2D spacial index of String values */
  public static class Module implements RamaModule {
    @Override
    public void define(Setup setup, Topologies topologies) {
      setup.declareDepot("*depot", Depot.random());

      StreamTopology s = topologies.stream("s");

      // declare the RTree
      final int dimensions = 2;
      RTree<String> rTree = new RTree<>(dimensions, 2, 1, "$$test");
      rTree.declarePStates(s);
      rTree.declareQueries(topologies);

      // ETL
      s.source("*depot").out("*c").macro(extractJavaFields("*c","*bounds","*value"))
	.macro(rTree.addObject("*bounds", "*value"));
    }
  }

  @Test
  public void allFeaturesTest() throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      cluster.launchModule(new Module(), new LaunchConfig(1, 1));

      final Depot depot = cluster.clusterDepot(Module.class.getName(), "*depot");
      final PState nodes = cluster.clusterPState(Module.class.getName(), "$$test__nodes");
      final PState root = cluster.clusterPState(Module.class.getName(), "$$test__root");
      final PState objects = cluster.clusterPState(Module.class.getName(), "$$test__objects");
      final QueryTopologyClient q = cluster.clusterQuery(Module.class.getName(), "objectsInBounds");

      final double[] origin = {0, 0};
      final double[] ones = {1, 1};
      final double[] twos = {2, 2};
      final double[] oneHundreds = {100, 100};
      final double[] twoHundreds = {200, 200};

      final MBR oneBounds = new MBR(origin, ones);
      final MBR twoBounds = new MBR(origin, twos);
      final MBR twoHundredBounds = new MBR(oneHundreds, twoHundreds);

      // Test that we can append an object in the root node and query for it.
      depot.append(new AddObject(oneBounds, "a"), AckLevel.ACK);
      {
	DepotPartitionInfo dpi = depot.getPartitionInfo(0);
	assertEquals(1, dpi.getEndOffset());

	RTree.Node node = root.selectOne(Path.stay());
	assertTrue(node instanceof RTree.Node);
	assertTrue(node.isLeaf());
	assertEquals(1, node.count());

	assertEquals(Arrays.asList("a"), q.invoke(oneBounds));
	assertEquals(Arrays.asList("a"), q.invoke(twoBounds));
	assertEquals(Arrays.asList(), q.invoke(twoHundredBounds));
      }

      // Append another object in the root node and query for it.
      depot.append(new AddObject(twoBounds, "b"), AckLevel.ACK);

      {
	DepotPartitionInfo dpi = depot.getPartitionInfo(0);
	assertEquals(2, dpi.getEndOffset());

	RTree.Node node = root.selectOne(Path.stay());
	assertTrue(node instanceof RTree.Node);
	assertTrue(node.isLeaf());
	assertEquals(2, node.count());

	assertEquals(Arrays.asList("a", "b"), q.invoke(oneBounds));
	assertEquals(Arrays.asList("a", "b"), q.invoke(twoBounds));
	assertEquals(Arrays.asList(), q.invoke(twoHundredBounds));
      }

      // Append another object when the root node is full and query for it.
      depot.append(new AddObject(twoBounds, "c"), AckLevel.ACK);

      {
	DepotPartitionInfo dpi = depot.getPartitionInfo(0);
	assertEquals(3, dpi.getEndOffset());

	RTree.Node node = root.selectOne(Path.stay());
	assertTrue(node instanceof RTree.Node);
	assertFalse(node.isLeaf());
	assertEquals(1, node.count());

	assertEquals(Arrays.asList("a", "b", "c"), q.invoke(oneBounds));
	assertEquals(Arrays.asList("a", "b", "c"), q.invoke(twoBounds));
	assertEquals(Arrays.asList(), q.invoke(twoHundredBounds));
      }
    }
  }
}
