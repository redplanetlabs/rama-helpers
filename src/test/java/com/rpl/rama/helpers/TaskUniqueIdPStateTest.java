package com.rpl.rama.helpers;

import com.rpl.rama.*;
import com.rpl.rama.module.StreamTopology;
import com.rpl.rama.ops.*;
import com.rpl.rama.test.InProcessCluster;
import com.rpl.rama.test.LaunchConfig;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import static org.junit.Assert.*;

public class TaskUniqueIdPStateTest {
  public static class Module implements RamaModule {
    @Override
    public void define(Setup setup, Topologies topologies) {
      StreamTopology s = topologies.stream("s");

      setup.declareDepot("*depot", Depot.hashBy(Ops.IDENTITY));
      s.pstate("$$p1", PState.mapSchema(String.class, PState.listSchema(Long.class)));
      s.pstate("$$p2", PState.mapSchema(String.class, PState.listSchema(Integer.class)));
      TaskUniqueIdPState p1 = new TaskUniqueIdPState("$$id1");
      TaskUniqueIdPState p2 = new TaskUniqueIdPState("$$id2").integerIds();
      p1.declarePState(s);
      p2.declarePState(s);

      s.source("*depot").out("*p")
       .hashPartition("$$p1", "*p")
       .macro(p1.genId("*id1"))
       .macro(p2.genId("*id2"))
       .compoundAgg("$$p1", CompoundAgg.map("*p", Agg.list("*id1")))
       .compoundAgg("$$p2", CompoundAgg.map("*p", Agg.list("*id2")));
    }
  }

  @Test
  public void allFeaturesTest() throws Exception {
    try (InProcessCluster cluster = InProcessCluster.create()) {
      cluster.launchModule(new Module(), new LaunchConfig(4, 1));

      Depot depot = cluster.clusterDepot(Module.class.getName(), "*depot");
      PState p1 = cluster.clusterPState(Module.class.getName(), "$$p1");
      PState p2 = cluster.clusterPState(Module.class.getName(), "$$p2");

      List<String> keys = Helpers.genHashingIndexKeys(4);

      for (int i = 0; i < 10; i++) {
        depot.append(keys.get(0));
        depot.append(keys.get(1));
        depot.append(keys.get(2));
        depot.append(keys.get(3));
      }

      // Make sure that the IDs are increasing and unique
      List<Long> ids = p1.selectOne(Path.key(keys.get(0)));
      assertEquals(ids, ids.stream().sorted().distinct().collect(Collectors.toList()));

      // Now make sure that they're non-unique across tasks
      assertEquals(ids, p1.selectOne(Path.key(keys.get(1))));
      assertEquals(ids, p1.selectOne(Path.key(keys.get(2))));
      assertEquals(ids, p1.selectOne(Path.key(keys.get(3))));

      List<Object> ids2 = p2.selectOne(Path.key(keys.get(0)));
      for(Object i: ids2) {
        assertTrue(i instanceof Integer);
      }
    }
  }
}
