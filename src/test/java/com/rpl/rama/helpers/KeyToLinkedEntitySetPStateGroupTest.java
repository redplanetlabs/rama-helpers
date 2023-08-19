package com.rpl.rama.helpers;

import com.rpl.rama.*;
import com.rpl.rama.test.*;
import com.rpl.rama.module.*;
import com.rpl.rama.RamaSerializable;

import clojure.lang.*;

import org.junit.Test;

import static org.junit.Assert.*;

public class KeyToLinkedEntitySetPStateGroupTest {
  public static class AddElement implements RamaSerializable {
    public String key;
    public String entity;
    public AddElement(String key, String entity) { this.key = key; this.entity = entity; }
  }

  public static class RemoveElement implements RamaSerializable {
    public String key;
    public String entity;
    public RemoveElement(String key, String entity) { this.key = key; this.entity = entity; }
  }

  public static class Module implements RamaModule {
    @Override
    public void define(Setup setup, Topologies topologies) {
      setup.declareDepot("*setDepot", Depot.random());

      StreamTopology s = topologies.stream("s");
      KeyToLinkedEntitySetPStateGroup p = new KeyToLinkedEntitySetPStateGroup("$$p", Object.class, Object.class);
      p.declarePStates(s);
      s.source("*setDepot").out("*c").subSource("*c",
        SubSource.create(AddElement.class)
                 .macro(TopologyUtils.extractJavaFields("*c", "*key", "*entity"))
                 .macro(p.addToLinkedSet("*key", "*entity")),
        SubSource.create(RemoveElement.class)
                 .macro(TopologyUtils.extractJavaFields("*c", "*key", "*entity"))
                 .macro(p.removeFromLinkedSet("*key", "*entity")));
    }
  }

  @Test
  public void allFeaturesTest() throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      cluster.launchModule(new Module(), new LaunchConfig(1, 1));

      Depot set = cluster.clusterDepot(Module.class.getName(), "*setDepot");
      PState p = cluster.clusterPState(Module.class.getName(), "$$p");
      PState pById = cluster.clusterPState(Module.class.getName(), "$$pById");

      set.append(new AddElement("a", "a"));
      assertEquals(0L, (long) p.selectOne(Path.key("a", "a")));
      assertEquals("a", pById.selectOne(Path.key("a", 0L)));

      set.append(new AddElement("a", "b"));
      assertEquals(1L, (long) p.selectOne(Path.key("a", "b")));
      assertEquals("b", pById.selectOne(Path.key("a", 1L)));

      set.append(new RemoveElement("a", "a"));
      assertNull(p.selectOne(Path.key("a", "a")));
      assertNull(pById.selectOne(Path.key("a", 0L)));

      set.append(new AddElement("a", "a"));
      assertEquals(2L, (long) p.selectOne(Path.key("a", "a")));
      assertEquals("a", pById.selectOne(Path.key("a", 2L)));

      set.append(new AddElement("a", "b"));
      assertEquals(3L, (long) p.selectOne(Path.key("a", "b")));
      assertEquals("b", pById.selectOne(Path.key("a", 3L)));
      assertNull(null, pById.selectOne(Path.key("a", 1L)));

      for (int i = 0; i < 20; i++) {
        set.append(new AddElement("b", String.valueOf(i)));
      }

      PersistentVector vec0to5 = PersistentVector.create((Object) PersistentVector.create(4, "0"));

      PersistentVector vec5to10 = PersistentVector.create(PersistentVector.create(5, "1"),
                                                          PersistentVector.create(6, "2"),
                                                          PersistentVector.create(7, "3"),
                                                          PersistentVector.create(8, "4"),
                                                          PersistentVector.create(9, "5"));

      assertTrue(vec0to5.equiv(pById.select(Path.key("b")
                                                .sortedMapRange(0L, 5L)
                                                .all())));

      assertTrue(vec5to10.equiv(pById.select(Path.key("b")
                                                 .sortedMapRange(5L, 10L)
                                                 .all())));
    }
  }
}
