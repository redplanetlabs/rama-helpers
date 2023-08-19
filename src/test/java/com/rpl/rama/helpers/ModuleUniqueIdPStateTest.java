package com.rpl.rama.helpers;

import com.rpl.rama.*;
import com.rpl.rama.module.StreamTopology;
import com.rpl.rama.test.InProcessCluster;
import com.rpl.rama.test.LaunchConfig;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import static org.junit.Assert.*;

public class ModuleUniqueIdPStateTest {
  public static class Module implements RamaModule {
    @Override
    public void define(Setup setup, Topologies topologies) {
      StreamTopology s = topologies.stream("s");

      // Ascending
      setup.declareDepot("*p", Depot.random());
      s.pstate("$$p", PState.mapSchema(Object.class, PState.listSchema(Object.class)));
      ModuleUniqueIdPState p = new ModuleUniqueIdPState("$$id");
      p.declarePState(s);

      s.source("*p").out("*key")
       .macro(p.genId("*id"))
       .compoundAgg("$$p", CompoundAgg.map("*key", Agg.list("*id")));

      // Descending
      setup.declareDepot("*pd", Depot.random());
      s.pstate("$$pd", PState.mapSchema(Object.class, PState.listSchema(Object.class)));
      ModuleUniqueIdPState pd = new ModuleUniqueIdPState("$$idD").descending();
      pd.declarePState(s);

      s.source("*pd").out("*key")
       .macro(pd.genId("*id"))
       .compoundAgg("$$pd", CompoundAgg.map("*key", Agg.list("*id")));
    }
  }

  // A second module identical to the first - does this work? Let's find out.
  public static class Module2 extends Module { }

  int desc(Long n1, Long n2) {
    return -Long.compare(n1, n2);
  }

  @Test
  public void allFeaturesTest() throws Exception {
    try (InProcessCluster cluster = InProcessCluster.create()) {
      cluster.launchModule(new Module(), new LaunchConfig(1, 1));
      cluster.launchModule(new Module2(), new LaunchConfig(1, 1));

      // Ascending
      Depot pDepot = cluster.clusterDepot(Module.class.getName(), "*p");
      PState p = cluster.clusterPState(Module.class.getName(), "$$p");

      for (int i = 0; i < 10; i++) {
        pDepot.append("a");
        pDepot.append("b");
      }

      List<Object> l = p.select(Path.all().all());
      List<Long> evens = (List<Long>) l.get(1);
      List<Long> odds = (List<Long>) l.get(3);

      // Messy, but checks if the elements are sorted (ascending) and distinct
      assertEquals(evens, evens.stream().sorted().distinct().collect(Collectors.toList()));
      assertTrue(evens.stream().allMatch((Long n) -> n % 2 == 0));

      assertEquals(odds, odds.stream().sorted().distinct().collect(Collectors.toList()));
      assertTrue(odds.stream().allMatch((Long n) -> n % 2 == 1));

      // Descending
      Depot pdDepot = cluster.clusterDepot(Module.class.getName(), "*pd");
      PState pd = cluster.clusterPState(Module.class.getName(), "$$pd");

      for (int i = 0; i < 10; i++) {
        pdDepot.append("a");
        pdDepot.append("b");
      }

      List<Object> l2 = pd.select(Path.all().all());
      List<Long> odds2 = (List<Long>) l2.get(1);
      List<Long> evens2 = (List<Long>) l2.get(3);

      assertEquals(evens2, evens2.stream().sorted(this::desc).distinct().collect(Collectors.toList()));
      assertTrue(evens2.stream().allMatch(n -> n % 2 == 0));

      assertEquals(odds2, odds2.stream().sorted(this::desc).distinct().collect(Collectors.toList()));
      assertTrue(odds2.stream().allMatch(n -> n % 2 == 1));

      // IDs can repeat across different modules
      Depot pDepot2 = cluster.clusterDepot(Module2.class.getName(), "*p");
      PState p2 = cluster.clusterPState(Module2.class.getName(), "$$p");

      pDepot2.append("a");
      assertEquals((long) p.selectOne(Path.key("a").first()),
                   (long) p2.selectOne(Path.key("a").first()));
    }
  }
}
