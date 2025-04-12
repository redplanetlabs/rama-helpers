package com.rpl.rama.helpers.spatial;

import com.rpl.rama.Depot;
import com.rpl.rama.PState;
import com.rpl.rama.QueryTopologyClient;
import com.rpl.rama.RamaModule;
import com.rpl.rama.RamaSerializable;
import com.rpl.rama.helpers.TopologyUtils.ExtractJavaField;
import static com.rpl.rama.helpers.TopologyUtils.extractJavaFields;
import com.rpl.rama.module.StreamTopology;
import com.rpl.rama.test.InProcessCluster;
import com.rpl.rama.test.LaunchConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Arrays;

import org.junit.Test;

public class RTreeTest {

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
      RTree rTree = new RTree(dimensions, "$$test", String.class);
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
      final PState p = cluster.clusterPState(Module.class.getName(), "$$test");
      final QueryTopologyClient q = cluster.clusterQuery(Module.class.getName(), "objectsInBounds");
      final double[] origin = {0, 0};
      final double[] ones = {1, 1};

      final MBR bounds = new MBR(origin, ones);
      depot.append(new AddObject(bounds, "a"));
      assertEquals(Arrays.asList("a"), q.invoke(bounds));
    }
  }
}
