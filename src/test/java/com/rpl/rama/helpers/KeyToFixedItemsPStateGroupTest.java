package com.rpl.rama.helpers;

import com.rpl.rama.*;
import com.rpl.rama.test.*;
import com.rpl.rama.module.*;
import com.rpl.rama.ops.Ops;

import org.junit.Test;

import static org.junit.Assert.*;

public class KeyToFixedItemsPStateGroupTest {
  public static class Module implements RamaModule {
    @Override
    public void define(Setup setup, Topologies topologies) {
      setup.declareDepot("*commandDepot", Depot.random());
            
      StreamTopology s = topologies.stream("s");
      KeyToFixedItemsPStateGroup p = new KeyToFixedItemsPStateGroup("$$p", 10, Object.class, Object.class)
                           .clearBatchSize(5);
      p.declarePStates(s);
      s.source("*commandDepot").out("*c").subSource("*c",
              SubSource.create(Actions.AddItem.class)
                      .macro(TopologyUtils.extractJavaFields("*c", "*key", "*item"))
                      .macro(p.addItem("*key", "*item")),
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

      depot.append(new Actions.AddItem("a", 1));
      assertEquals(1, (int) p.selectOne(Path.key("a", Long.MAX_VALUE)));

      // Test non-uniqueness
      depot.append(new Actions.AddItem("a", 1));
      assertEquals(1, (int) p.selectOne(Path.key("a", Long.MAX_VALUE)));
      assertEquals(1, (int) p.selectOne(Path.key("a", Long.MAX_VALUE-1)));

      // Test clear
      depot.append(new Actions.ClearItems("a"));
      assertEquals(0, (int) p.selectOne(Path.key("a").view(Ops.SIZE)));

      // Remove an item by ID
      depot.append(new Actions.AddItem("a", 1));
      Object id = p.selectOne(Path.key("a").mapKeys());
      assertNotNull(p.selectOne(Path.key("a", id)));
      depot.append(new Actions.RemoveItemById("a", id));
      assertNull(p.selectOne(Path.key("a", id)));

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

      depot.append(new Actions.ClearItems("a"));
      assertEquals(0, (int) p.selectOne(Path.key("a").view(Ops.SIZE)));
    }
  }

}
