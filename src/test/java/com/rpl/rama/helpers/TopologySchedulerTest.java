package com.rpl.rama.helpers;

import static org.junit.Assert.*;

import java.io.Closeable;
import java.util.*;

import org.junit.Test;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.ops.*;
import com.rpl.rama.test.*;

import static com.rpl.rama.helpers.TestHelpers.*;

public class TopologySchedulerTest {
  public static class OrderingModule implements RamaModule {
    @Override
    public void define(Setup setup, Topologies topologies) {
      setup.declareDepot("*depot", Depot.random());

      StreamTopology s = topologies.stream("s");
      TopologyScheduler t = new TopologyScheduler("$$p");
      t.declarePStates(s);
      s.source("*depot").out("*expiration")
       .macro(t.scheduleItem("*expiration", "item"));
    }
  }

  @Test
  public void orderingTest() throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      cluster.launchModule(new OrderingModule(), new LaunchConfig(1, 1));

      Depot depot = cluster.clusterDepot(OrderingModule.class.getName(), "*depot");
      PState expirations = cluster.clusterPState(OrderingModule.class.getName(), "$$pExpirations");

      Random r = new Random();
      List<Long> appends = new ArrayList();
      for(int i=0; i<500; i++) {
        long l = Math.abs(r.nextLong());
        appends.add(l);
        depot.append(l);
      }

      for(int i=0; i<20; i++) {
        int idx = r.nextInt(appends.size());
        long l = appends.get(idx);
        appends.add(l);
        depot.append(l);
      }

      Collections.sort(appends);

      assertEquals(appends, expirations.select(Path.mapKeys().first()));
    }
  }

  public static class MicrobatchProcessingModule implements RamaModule {
    @Override
    public void define(Setup setup, Topologies topologies) {
      setup.declareDepot("*depot", Depot.hashBy(Ops.FIRST));

      MicrobatchTopology mb = topologies.microbatch("mb");
      TopologyScheduler t = new TopologyScheduler("$$p");
      t.declarePStates(mb);
      mb.pstate("$$m", PState.mapSchema(String.class,
                                        PState.fixedKeysSchema("count", Long.class,
                                                               "time", Long.class)));
      mb.source("*depot").out("*microbatch")
        .anchor("Root")
        .macro(t.handleExpirations("*k", "*currTime",
          Block.each(Ops.IDENTITY, "*k").out("*k2")
               .anchor("SomeAnchor"))) // verify anchors within the expiration handling are usable outside

        .hook("SomeAnchor")
        .compoundAgg("$$m", CompoundAgg.map("*k2",
                                            CompoundAgg.map("count", Agg.count(),
                                                            "time", Agg.last("*currTime"))))

        .hook("Root")
        .explodeMicrobatch("*microbatch").out("*data")
        .each(Ops.EXPAND, "*data").out("*k", "*expiration")
        .macro(t.scheduleItem("*expiration", "*k"));
    }
  }

  private static boolean equals(Object a1, Object a2) {
    return a1 == null && a2 == null ||
           a1 != null && a1.equals(a2);
  }

  @Test
  public void microbatchProcessingTest() throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create();
        Closeable simTime = TopologyUtils.startSimTime()) {
      cluster.launchModule(new MicrobatchProcessingModule(), new LaunchConfig(4, 2));

      Depot depot = cluster.clusterDepot(MicrobatchProcessingModule.class.getName(), "*depot");
      PState m = cluster.clusterPState(MicrobatchProcessingModule.class.getName(), "$$m");

      depot.append(Arrays.asList("a", 11));
      depot.append(Arrays.asList("b", 11));
      depot.append(Arrays.asList("a", 10));
      depot.append(Arrays.asList("b", 10));
      depot.append(Arrays.asList("c", 10));
      depot.append(Arrays.asList("a", 10));
      depot.append(Arrays.asList("a", 11));
      depot.append(Arrays.asList("a", 100));
      depot.append(Arrays.asList("a", 9));

      cluster.waitForMicrobatchProcessedCount(MicrobatchProcessingModule.class.getName(), "mb", 9);

      Thread.sleep(200);
      attainStableCondition(() -> m.selectOne(Path.key("a", "count")) == null);

      TopologyUtils.advanceSimTime(8);
      Thread.sleep(200);
      attainStableCondition(() -> m.selectOne(Path.key("a", "count")) == null);

      TopologyUtils.advanceSimTime(1);
      Thread.sleep(200);
      attainStableCondition(() -> equals(1L, m.selectOne(Path.key("a", "count"))));
      attainStableCondition(() -> m.selectOne(Path.key("b", "count")) == null);

      TopologyUtils.advanceSimTime(1);
      Thread.sleep(200);
      attainStableCondition(() -> equals(3L, m.selectOne(Path.key("a", "count"))));
      attainStableCondition(() -> equals(1L, m.selectOne(Path.key("b", "count"))));
      attainStableCondition(() -> equals(1L, m.selectOne(Path.key("c", "count"))));

      TopologyUtils.advanceSimTime(1);
      attainStableCondition(() -> equals(5L, m.selectOne(Path.key("a", "count"))));
      attainStableCondition(() -> equals(2L, m.selectOne(Path.key("b", "count"))));
      attainStableCondition(() -> equals(1L, m.selectOne(Path.key("c", "count"))));


      depot.append(Arrays.asList("a", 50));
      depot.append(Arrays.asList("a", 150));

      cluster.waitForMicrobatchProcessedCount(MicrobatchProcessingModule.class.getName(), "mb", 11);

      TopologyUtils.advanceSimTime(39);
      Thread.sleep(200);
      attainStableCondition(() -> equals(6L, m.selectOne(Path.key("a", "count"))));

      TopologyUtils.advanceSimTime(50);
      Thread.sleep(200);
      attainStableCondition(() -> equals(7L, m.selectOne(Path.key("a", "count"))));

      TopologyUtils.advanceSimTime(50);
      Thread.sleep(200);
      attainStableCondition(() -> equals(8L, m.selectOne(Path.key("a", "count"))));
    }
  }

  public static class StreamProcessingModule implements RamaModule {
    @Override
    public void define(Setup setup, Topologies topologies) {
      setup.declareDepot("*depot", Depot.random());
      setup.declareTickDepot("*tick", 1000);

      StreamTopology s = topologies.stream("s");
      s.pstate("$$p", PState.mapSchema(String.class, Long.class));
      TopologyScheduler t = new TopologyScheduler("$$p");
      t.declarePStates(s);

      s.source("*depot").out("*data")
       .each(Ops.EXPAND, "*data").out("*k", "*expiration")
       .macro(t.scheduleItem("*expiration", "*k"));

      s.source("*tick")
       .macro(t.handleExpirations("*k", "*currTime",
         Block.hashPartition("*k")
              .compoundAgg("$$p", CompoundAgg.map("*k", Agg.count()))
              .anchor("Anchor")))
       .hook("Anchor"); // just verify it compiles
    }
  }

  @Test
  public void streamProcessingTest() throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create();
        Closeable simTime = TopologyUtils.startSimTime()) {
      cluster.launchModule(new StreamProcessingModule(), new LaunchConfig(4, 2));

      Depot depot = cluster.clusterDepot(StreamProcessingModule.class.getName(), "*depot");
      PState p = cluster.clusterPState(StreamProcessingModule.class.getName(), "$$p");

      depot.append(Arrays.asList("a", 11));
      depot.append(Arrays.asList("b", 11));
      depot.append(Arrays.asList("a", 9));
      depot.append(Arrays.asList("a", 11));
      depot.append(Arrays.asList("a", 10));
      depot.append(Arrays.asList("b", 10));
      depot.append(Arrays.asList("a", 10));

      TopologyUtils.advanceSimTime(8);
      Thread.sleep(200);
      attainStableCondition(() -> p.selectOne(Path.key("a")) == null);

      TopologyUtils.advanceSimTime(1);
      Thread.sleep(200);
      attainStableCondition(() -> equals(1L, p.selectOne(Path.key("a"))));
      assertNull(p.selectOne(Path.key("b")));

      TopologyUtils.advanceSimTime(1);
      Thread.sleep(200);
      attainStableCondition(() -> equals(3L, p.selectOne(Path.key("a"))));
      attainStableCondition(() -> equals(1L, p.selectOne(Path.key("b"))));

      TopologyUtils.advanceSimTime(1);
      Thread.sleep(200);
      attainStableCondition(() -> equals(5L, p.selectOne(Path.key("a"))));
      attainStableCondition(() -> equals(2L, p.selectOne(Path.key("b"))));
    }
  }
}
