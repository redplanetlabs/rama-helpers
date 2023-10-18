package com.rpl.rama.helpers;

import com.rpl.rama.*;
import com.rpl.rama.module.StreamTopology;
import com.rpl.rama.ops.Ops;
import com.rpl.rama.test.InProcessCluster;
import com.rpl.rama.test.LaunchConfig;

import org.junit.Test;

import static org.junit.Assert.*;

public class KeyToUniqueFixedItemsPStateGroupTest {

  public static class Module implements RamaModule {
    @Override
    public void define(Setup setup, Topologies topologies) {
      setup.declareDepot("*commandDepot", Depot.random());

      StreamTopology s = topologies.stream("s");
      KeyToUniqueFixedItemsPStateGroup p = new KeyToUniqueFixedItemsPStateGroup("$$p", 10, Object.class, Object.class)
                            .clearBatchSize(5);
      p.declarePStates(s);
      s.source("*commandDepot").out("*c").subSource("*c",
        SubSource.create(Actions.AddItem.class)
                 .macro(TopologyUtils.extractJavaFields("*c", "*key", "*item"))
                 .macro(p.addItem("*key", "*item")),
        SubSource.create(Actions.RemoveItem.class)
                 .macro(TopologyUtils.extractJavaFields("*c", "*key", "*item"))
                 .macro(p.removeItem("*key", "*item")),
        SubSource.create(Actions.RemoveItemById.class)
                 .macro(TopologyUtils.extractJavaFields("*c", "*key", "*id"))
                 .macro(p.removeItemById("*key", "*id")),
        SubSource.create(Actions.RemoveKey.class)
                 .macro(TopologyUtils.extractJavaFields("*c", "*key"))
                 .macro(p.removeKey("*key")),
        SubSource.create(Actions.ClearItems.class)
                 .macro(TopologyUtils.extractJavaFields("*c", "*key"))
                 .macro(p.clearItems("*key"))
      );
    }
  }

  @Test
  public void allFeaturesTest() throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      cluster.launchModule(new Module(), new LaunchConfig(1, 1));

      Depot depot = cluster.clusterDepot(Module.class.getName(), "*commandDepot");
      PState p = cluster.clusterPState(Module.class.getName(), "$$p");
      PState pR = cluster.clusterPState(Module.class.getName(), "$$pReverse");

      // Basic add an item
      depot.append(new Actions.AddItem("a", 1));
      assertEquals(1, (int) p.selectOne(Path.key("a", Long.MAX_VALUE)));
      assertEquals(Long.MAX_VALUE, (long) pR.selectOne(Path.key("a", 1)));

      // Test uniqueness constraint
      depot.append(new Actions.AddItem("a", 1));
      assertNull(p.selectOne(Path.key("a", Long.MAX_VALUE)));
      assertEquals(1, (int) p.selectOne(Path.key("a", Long.MAX_VALUE-1)));
      assertEquals(Long.MAX_VALUE-1, (long) pR.selectOne(Path.key("a", 1)));

      // Basic remove an item
      depot.append(new Actions.RemoveItem("a", 1));
      assertNull(p.selectOne(Path.key("a", Long.MAX_VALUE)));
      assertNull(pR.selectOne(Path.key("a", 1)));

      // Remove an item by ID
      depot.append(new Actions.AddItem("a", 1));
      long id = pR.selectOne(Path.key("a", 1));
      depot.append(new Actions.RemoveItemById("a", id));
      assertNull(p.selectOne(Path.key("a", id)));
      assertNull(pR.selectOne(Path.key("a", 1)));

      // Test capacity constraint
      for (int i = 0; i < 20; i++) {
        depot.append(new Actions.AddItem("a", i));
      }
      assertEquals(10, (int) p.selectOne(Path.key("a").view(Ops.SIZE)));

      // Removing everything
      for (int i = 0; i < 10; i++) {
        depot.append(new Actions.AddItem("b", i));
      }
      depot.append(new Actions.RemoveKey("b"));
      assertNull(p.selectOne(Path.key("b")));
      assertNull(pR.selectOne(Path.key("b")));

      depot.append(new Actions.ClearItems("a"));
      assertEquals(0, (int) p.selectOne(Path.key("a").view(Ops.SIZE)));

      // TODO: <<<<>>>>
      //   - verify removeKey removes from pstateReverse as well
      //   - check usage of entityIdFunction
    }
  }
}
