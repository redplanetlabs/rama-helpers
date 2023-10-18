package com.rpl.rama.helpers;

import com.rpl.rama.*;
import com.rpl.rama.module.StreamTopology;
import com.rpl.rama.ops.Ops;
import com.rpl.rama.ops.RamaFunction1;
import com.rpl.rama.test.InProcessCluster;
import com.rpl.rama.test.LaunchConfig;

import org.junit.Test;

import static org.junit.Assert.*;

import java.util.Arrays;

public class KeyToUniqueFixedItemsPStateGroupTest {

  public static class Module implements RamaModule {
    public RamaFunction1 entityIdFn = null;

    @Override
    public void define(Setup setup, Topologies topologies) {
      setup.declareDepot("*commandDepot", Depot.random());

      StreamTopology s = topologies.stream("s");
      KeyToUniqueFixedItemsPStateGroup p = new KeyToUniqueFixedItemsPStateGroup("$$p", 10, Object.class, Object.class)
                            .clearBatchSize(5);
      if(entityIdFn!=null) p.entityIdFunction(Object.class, entityIdFn);
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
        SubSource.create(Actions.RemoveItemByEntityId.class)
                 .macro(TopologyUtils.extractJavaFields("*c", "*key", "*id"))
                 .macro(p.removeItemByEntityId("*key", "*id")),
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
    }
  }

  @Test
  public void entityIdFnTest() throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      Module m = new Module();
      m.entityIdFn = Ops.FIRST;
      cluster.launchModule(m, new LaunchConfig(1, 1));

      Depot depot = cluster.clusterDepot(Module.class.getName(), "*commandDepot");
      PState p = cluster.clusterPState(Module.class.getName(), "$$p");
      PState pR = cluster.clusterPState(Module.class.getName(), "$$pReverse");

      Object item1 = Arrays.asList("x", 1);
      Object item2 = Arrays.asList("y", 2);
      Object item3 = Arrays.asList("x", 3);
      Object item4 = Arrays.asList("z", 4);

      depot.append(new Actions.AddItem("a", item1));
      assertEquals(item1, p.selectOne(Path.key("a", Long.MAX_VALUE)));
      assertEquals(Long.MAX_VALUE, (long) pR.selectOne(Path.key("a", "x")));

      depot.append(new Actions.AddItem("a", item2));
      assertEquals(item2, p.selectOne(Path.key("a", Long.MAX_VALUE - 1)));
      assertEquals(Long.MAX_VALUE - 1, (long) pR.selectOne(Path.key("a", "y")));

      depot.append(new Actions.AddItem("a", item3));
      assertEquals(item3, p.selectOne(Path.key("a", Long.MAX_VALUE - 2)));
      assertNull(p.selectOne(Path.key("a", Long.MAX_VALUE)));
      assertEquals(Long.MAX_VALUE - 2, (long) pR.selectOne(Path.key("a", "x")));
      assertEquals(2, (int) p.selectOne(Path.key("a").view(Ops.SIZE)));
      assertEquals(2, (int) pR.selectOne(Path.key("a").view(Ops.SIZE)));

      depot.append(new Actions.AddItem("a", item4));
      assertEquals(3, (int) p.selectOne(Path.key("a").view(Ops.SIZE)));
      assertEquals(3, (int) pR.selectOne(Path.key("a").view(Ops.SIZE)));

      depot.append(new Actions.RemoveItemByEntityId("a", "x"));
      assertEquals(2, (int) p.selectOne(Path.key("a").view(Ops.SIZE)));
      assertEquals(2, (int) pR.selectOne(Path.key("a").view(Ops.SIZE)));
      assertNull(pR.selectOne(Path.key("a", "x")));

      depot.append(new Actions.RemoveItem("a", item4));
      assertEquals(1, (int) p.selectOne(Path.key("a").view(Ops.SIZE)));
      assertEquals(1, (int) pR.selectOne(Path.key("a").view(Ops.SIZE)));
      assertNull(pR.selectOne(Path.key("a", "z")));
    }
  }
}
