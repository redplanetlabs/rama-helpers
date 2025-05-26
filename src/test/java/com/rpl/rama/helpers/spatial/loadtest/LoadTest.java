package com.rpl.rama.helpers.spatial.loadtest;

import com.rpl.rama.Block;
import com.rpl.rama.Case;
import com.rpl.rama.Expr;
import com.rpl.rama.PState;
import com.rpl.rama.RamaModule;
import com.rpl.rama.helpers.spaatial.loadtest.LoadTestStateMachine;
import com.rpl.rama.helpers.spaatial.loadtest.LoadTestStateMachine.LoadTestState;
import com.rpl.rama.module.MicrobatchTopology;
import com.rpl.rama.ops.Ops;

import org.slf4j.LoggerFactory;

public class LoadTest {

  public static final Object LOGGER = LoggerFactory.getLogger(LoadTest.class);

  public static class Module implements RamaModule {

    LoadTestStateMachine statemachine = new LoadTestStateMachine();

    @Override
    public void define(Setup setup, Topologies topologies) {
      statemachine.stateMachine.define(
        setup,
        topologies,
        LoadTestStateMachine.LoadTestState.LOAD_DATA);

      setup.declareObject("*appendsInFlight", 0);
      MicrobatchTopology m = topologies.microbatch("m");

      m.pstate("$$inProgressAppends", PState.mapSchema(Long.class, Long.class));

      m.source("*smDepot").out("*microbatch")
          .batchBlock(
            Block
            .explodeMicrobatch("*microbatch").out("*state")
            .cond(
              Case.create(
                new Expr(Ops.EQUAL,
                         "*state",
                         LoadTestStateMachine.LoadTestState.LOAD_DATA))
              .each(Ops.LOG_DEBUG, LOGGER, "LOAD DATA")
              .allPartition()
              .each(Ops.LOG_DEBUG, LOGGER, "LOAD DATA DONE"),
              Case.create(
                new Expr(Ops.EQUAL,
                         "*state",
                         LoadTestStateMachine.LoadTestState.TIME_PROCESSING))
              .each(Ops.LOG_DEBUG, LOGGER, "TIME_PROCESSING"),
              Case.create(
                new Expr(Ops.EQUAL,
                         "*state",
                         LoadTestStateMachine.LoadTestState.QUERY_PERFORMANCE))
              .each(Ops.LOG_DEBUG, LOGGER, "QUERY_PERFORMANCE"))
                      );
    }
  }

  // public void declareTopology(TopologyBuilder builder) {
  //     // Create and configure the state machine
  //     StateMachineRunner<LoadTestState, LoadTestContext> stateMachine =
  //         LoadTestStateMachine.create();

    //     // Build the topology
    //     stateMachine.buildTopology(builder);

    //     // Start the state machine
    //     stateMachine.start(LoadTestState.LOAD_DATA);

    //     // Access queries for monitoring
    //     StateMachineQueries<LoadTestState, LoadTestContext> queries = stateMachine.getQueries();
    //     Query<Integer, CoordinationState<LoadTestState, LoadTestContext>> stateQuery =
    //         queries.getCoordinationStateQuery();
    // }
}
