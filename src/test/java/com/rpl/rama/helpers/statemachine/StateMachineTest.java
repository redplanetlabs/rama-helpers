package com.rpl.rama.helpers.statemachine;

import java.time.Duration;

import com.rpl.rama.Block;
import com.rpl.rama.Case;
import com.rpl.rama.Expr;
import com.rpl.rama.PState;
import com.rpl.rama.Path;
import com.rpl.rama.RamaModule;
import com.rpl.rama.RamaSerializable;
import com.rpl.rama.helpers.statemachine.coordination.PartitionProgress;
import com.rpl.rama.helpers.statemachine.core.StateMachine;
import com.rpl.rama.helpers.statemachine.core.StateMachineConfig;
import com.rpl.rama.helpers.statemachine.core.StateMachineState;
import com.rpl.rama.module.MicrobatchTopology;
import com.rpl.rama.ops.Ops;
import com.rpl.rama.test.InProcessCluster;
import com.rpl.rama.test.LaunchConfig;

import org.junit.jupiter.api.Test;
import static org.junit.Assert.assertEquals;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StateMachineTest {
  public static final Logger LOGGER =
      LoggerFactory.getLogger(StateMachineTest.class);

  public static class Module implements RamaModule {

    public static enum SMState implements RamaSerializable {
      DURATION_TEST,
      ALL_PROGRESS_TEST,
      FINAL
    }

    public static enum SMSignal implements RamaSerializable {
      TEST_SIGNAL
    }

    public StateMachine<SMState, SMSignal> stateMachine =
        new StateMachine<SMState, SMSignal>(
          new StateMachineConfig.Builder<SMState, SMSignal>()

          .state(SMState.DURATION_TEST)
          .afterDuration(Duration.ofSeconds(1), SMState.ALL_PROGRESS_TEST)
          .done()

          .state(SMState.ALL_PROGRESS_TEST)
          .onAllSignalled(SMSignal.TEST_SIGNAL,
                          Duration.ofSeconds(1),
                          SMState.FINAL)
          .done()

          .state(SMState.FINAL)
          .onTimeout(Duration.ofSeconds(100), false, SMState.FINAL)
          .done()

          .build())
        ;

    @Override
    public void define(Setup setup, Topologies topologies) {
      stateMachine.define(setup, topologies, SMState.DURATION_TEST);

      LOGGER.debug("define: state machine config: " +
                   stateMachine.getStateMachineConfig());

      setup.declareObject("*appendsInFlight", 0);
      MicrobatchTopology m = topologies.microbatch("m");

      m.pstate("$$inProgressAppends",
               PState.mapSchema(Long.class, Long.class));

      final String smStateVar = "*smState";

      m.source("*smDepot").out("*microbatch")
          .each(Ops.LOG_DEBUG, LOGGER, "StateMachinTest task")
          .batchBlock(
            Block
            .explodeMicrobatch("*microbatch").out("*mbValue")

            // TODO need state machine state
            .each(Ops.CURRENT_TASK_ID).out("*taskId")
            .directPartition(0)
            .localSelect("$$sm", Path.stay()).out(smStateVar)
            .each(StateMachineState<SMState>::getCurrentState,
                  smStateVar).out("*state")
            .directPartition("*taskId")
            .each(Ops.LOG_DEBUG, LOGGER,
                  new Expr(Ops.TO_STRING, "task state: ", "*state"))
            .cond(
              Case.create(
                new Expr(Ops.EQUAL, "*state", SMState.DURATION_TEST))
              .each(Ops.LOG_DEBUG, LOGGER, "DURATION_TEST")
              .allPartition()
              .each(Ops.LOG_DEBUG, LOGGER, "DURATION_TEST DONE"),

              Case.create(
                new Expr(Ops.EQUAL, "*state", SMState.ALL_PROGRESS_TEST))
              .each(Ops.LOG_DEBUG, LOGGER, "ALL_PROGRESS_TEST")
              .each(StateMachineState<SMState>::elapsedDuration,
                    smStateVar).out("*stateDuration")
              .each(Duration::toMillis,
                    "*stateDuration").out("*durationMillis")
              .allPartition()
              .each(Ops.IDENTITY,
                    PartitionProgress.PartitionStatus.WORKING).out("*status")
              .each(Ops.CURRENT_TASK_ID).out("*localTaskId")
              .macro(stateMachine.madeProgress("*localTaskId",
                                               "*state",
                                               "*status"))
              .each(Ops.LOG_DEBUG, LOGGER, "XXX")
              .each(Ops.LOG_DEBUG, LOGGER,
                    new Expr(Ops.TO_STRING,
                             "durationMillis: ", "*durationMillis"))
              .ifTrue(new Expr(Ops.GREATER_THAN, "*durationMillis", 1000L),
                      Block
                      .each(Ops.LOG_DEBUG, LOGGER, "set signal")
                      .each(Ops.IDENTITY, SMSignal.TEST_SIGNAL).out("*signal")
                      .macro(stateMachine.setSignal("*localTaskId", "*signal"))
                      .each(Ops.LOG_DEBUG, LOGGER, "set signal done"))
              .each(Ops.LOG_DEBUG, LOGGER, "ALL_PROGRESS_TEST DONE"),

              Case.create(new Expr(Ops.IDENTITY, Boolean.TRUE))
              .each(Ops.LOG_DEBUG, LOGGER,
                    new Expr(Ops.TO_STRING, "Unhandled: ", "*state"))
                  )
            .each(Ops.LOG_DEBUG, LOGGER, "StateMachinTest task done"));
    }
  }

  @Test
  public void stateMachineTest() throws Exception {

    try (InProcessCluster cluster = InProcessCluster.create()) {
      final RamaModule module = new Module();
      cluster.launchModule(module, new LaunchConfig(4, 3));
      final PState smState = cluster.clusterPState(Module.class.getName(), "$$sm");

      Thread.sleep(10000);

      StateMachineState<Module.SMState> state = smState.selectOne(Path.stay());
      assertEquals(Module.SMState.FINAL, state.currentState);

    }
  }

}
