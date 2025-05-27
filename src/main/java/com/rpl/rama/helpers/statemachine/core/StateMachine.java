package com.rpl.rama.helpers.statemachine.core;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Iterator;

import com.rpl.rama.Agg;
import com.rpl.rama.Block;
import com.rpl.rama.Case;
import com.rpl.rama.Depot;
import com.rpl.rama.Expr;
import com.rpl.rama.Helpers;
import com.rpl.rama.Path;
import com.rpl.rama.RamaSerializable;
import com.rpl.rama.RamaModule.Setup;
import com.rpl.rama.RamaModule.Topologies;
import com.rpl.rama.helpers.statemachine.coordination.PartitionProgress;
import com.rpl.rama.helpers.statemachine.coordination.SignalUpdate;
import com.rpl.rama.helpers.statemachine.core.StateConfig.OnAllSignalled;
import com.rpl.rama.module.MicrobatchTopology;
import com.rpl.rama.ops.Ops;
import com.rpl.rama.ops.RamaFunction1;
import com.rpl.rama.ops.RamaFunction2;
import com.rpl.rama.ops.RamaFunction3;

import static com.rpl.rama.helpers.TopologyUtils.extractJavaFields;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import clojure.lang.APersistentMap;
import clojure.lang.PersistentVector;
import rpl.shaded.scala.collection.immutable.Vector;

public class StateMachine<State extends Enum<State>,
                                Signal extends Enum<Signal>>
    implements RamaSerializable {

  public static final Logger LOGGER =
      LoggerFactory.getLogger(StateMachine.class);

  private StateMachineConfig<State, Signal> stateMachineConfig;

  public StateMachine(StateMachineConfig<State, Signal> stateMachineConfig) {
    this.stateMachineConfig = stateMachineConfig;
  }

  public static class CurrentState<State extends Enum<State>> {
    public State stateValue;
  }

  public StateConfig<State, Signal> stateConfig(State state) {
    return stateMachineConfig.stateConfig(state);
  }

  public StateMachineConfig<State, Signal> getStateMachineConfig() {
    return stateMachineConfig;
  }

  public void define(Setup setup, Topologies topologies, State initState) {
    setup.declareTickDepot("*smDepot", 100);  // 100m
    setup.declareDepot("*smCoordDepot", Depot.disallow());  // 100ms

    MicrobatchTopology sm = topologies.microbatch("sm");
    StateMachineState<State> smState
        = new StateMachineState<State>(initState);
    sm.pstate("$$sm", StateMachineState.class).global().initialValue(smState);
    sm.pstate("$$smProgress", PartitionProgress.class);
    sm.pstate("$$smSignal", Object.class);

    final String smStateVar = Helpers.genVar("smState");
    final String newStateVar = Helpers.genVar("newState");
    sm.source("*smDepot").batchBlock(
      Block
      .each(Ops.LOG_DEBUG, LOGGER, "StateMachine coord")
      .localSelect("$$sm", Path.stay()).out(smStateVar)
      .each(StateMachineState<State>::getCurrentState,
            smStateVar).out("*stateValue")
      .each(StateMachine<State, Signal>::stateConfig, this,
            "*stateValue").out("*stateConfig")
      .each(StateConfig<State, Signal>::getTransitions,
            "*stateConfig").out("*transitionIter")
      .each(Ops.LOG_DEBUG, LOGGER,
            new Expr(Ops.TO_STRING, "StateMachine coord: ", "*stateValue"))
      .loop(
        Block
        .each(Iterator<Transition<State>>::hasNext,
              "*transitionIter").out("*hasNext")
        .ifTrue("*hasNext",
                Block.each(Iterator<Transition<State>>::next,
                           "*transitionIter").out("*transition")
                .each(Ops.LOG_DEBUG, LOGGER,
                      new Expr(Ops.TO_STRING,
                               "Check transition: ", "*transition"))
                .macro(nextState(smStateVar, "*transition", newStateVar))
                .ifTrue(
                  new Expr(Ops.IS_NULL, newStateVar),
                  Block.continueLoop(),
                  Block
                  .each(StateMachineState<State>::setState,
                        smStateVar, newStateVar).out(smStateVar)
                  .localTransform("$$sm", Path.termVal(smStateVar))))));

    sm.source("*smCoordDepot").out("*mb")
        .explodeMicrobatch("*mb").out("*update")
        .each(Ops.LOG_DEBUG, LOGGER,
              new Expr(Ops.TO_STRING, "Process update", "*update"))
        .cond(Case.create(
          new Expr(Ops.IS_INSTANCE_OF, SignalUpdate.class, "*update"))
              .each(Ops.LOG_DEBUG, LOGGER,
                    new Expr(Ops.TO_STRING,
                             "process signal update: ", "*update"))
              .macro(extractJavaFields("*update", "*taskId", "*signal"))
              .localTransform("$$smSignal",
                              Path.key("*taskId").termVal("*signal")),

              Case.create(
                new Expr(Ops.IS_INSTANCE_OF,
                         PartitionProgress.class,
                         "*update"))
              .each(Ops.LOG_DEBUG, LOGGER,
                    new Expr(Ops.TO_STRING,
                             "process progress update: ", "*update"))
              .macro(extractJavaFields("*update", "*taskId"))
              .localTransform("$$smProgress",
                              Path.key("*taskId").termVal("*update")),

              Case.create(true)
              .each(Ops.LOG_ERROR, LOGGER,
                    new Expr(Ops.TO_STRING,
                             "Invalid append to state machine coordination depot: ",
                             "*update")))
        ;

    topologies.query("partitionStates").out("*allProgress")
        .each(Ops.LOG_DEBUG, LOGGER,"partitionStates")
        .allPartition()
        .localSelect("$$smProgress", Path.stay()).out("*progress")
        .each(Ops.LOG_DEBUG, LOGGER,
              new Expr(Ops.TO_STRING, "partitionStates: ", "*progress"))
        .each(Ops.CURRENT_TASK_ID).out("*taskId")
        .originPartition()
        .agg(Agg.map("*taskId", "*progress")).out("*allProgress");
  }

  private Block nextState(final String smStateVar,
                          final String transitionVar,
                          final String newStateVar) {
    return Block
        .each((RamaFunction1<Transition<State>, TransitionType>)
              Transition::type, transitionVar).out("*type")
        .each(Ops.LOG_DEBUG, LOGGER,
              new Expr(Ops.TO_STRING,
                       "nextState for transition: ", transitionVar,
                       ", type: ", "*type"))
        .cond(
          Case
          .create(new Expr(Ops.EQUAL, TransitionType.TIMEOUT, "*type"))
          .macro(timeoutTransition(smStateVar, transitionVar, newStateVar)),
          Case
          .create(new Expr(Ops.EQUAL, TransitionType.DURATION, "*type"))
          .macro(duration(smStateVar, transitionVar, newStateVar)),
          Case
          .create(new Expr(Ops.EQUAL, TransitionType.ALL_SIGNALLED, "*type"))
          .macro(allSignalled(smStateVar, transitionVar, newStateVar)));
  }

  private Block timeoutTransition(final String smStateVar,
                                  final String transitionVar,
                                  final String newStateVar) {
    return Block.each(Ops.IDENTITY, null).out(newStateVar);
  }

  private Block duration(final String smStateVar,
                         final String transitionVar,
                         final String newStateVar) {

    return Block
        .each(StateMachineState<State>::elapsedDuration,
              smStateVar).out("*elapsedDuration")
        .each(StateConfig.AfterDuration<State>::isExpired,
              transitionVar, "*elapsedDuration").out("*isExpired")
        .each(Ops.LOG_DEBUG, LOGGER,
              new Expr(Ops.TO_STRING, "duratuion: ", "*elapsedDuration",
                       ", isExpired: ", "*isExpired",
                       ", transition: ", transitionVar))
        .ifTrue("*isExpired",
                Block
                .each(Transition<State>::targetState,
                      transitionVar).out(newStateVar)
                .each(Ops.LOG_DEBUG, LOGGER,
              new Expr(Ops.TO_STRING, "duratuion expired: ", newStateVar)),
                Block.each(Ops.IDENTITY, null).out(newStateVar));
  }

  private static <State extends Enum<State>,
                                Signal extends Enum<Signal>>
  Boolean isAllSignalled(
    StateConfig.OnAllSignalled<State, Signal> transition,
    APersistentMap signals) {

    if (signals == null) {
      return false;
    } else {
      LOGGER.debug("Signals: ", signals.toString());

      return ((Collection<Signal>)(signals.values()))
          .stream()
          .reduce(Boolean.TRUE,
                  (res, p) ->
                  (p == null || p != transition.signal) ? false : res,
                  (Boolean res, Boolean other) -> res && other);
    }
  }

  private static <State extends Enum<State>,
                                Signal extends Enum<Signal>>
  Boolean isAnyTimedOut(
    StateConfig.OnAllSignalled<State, Signal> transition,
    APersistentMap progress) {
    long timeoutMillis = Instant.now().minus(transition.timeout).toEpochMilli();

    if (progress == null) {
      return false;
    } else {
      LOGGER.debug("Progress: ", progress.toString());

      return ((Collection<PartitionProgress<State>>)(progress.values()))
          .stream()
          .reduce(Boolean.TRUE,
                  (res, p) ->
                  (p == null || p.timestamp < timeoutMillis) ? false : res,
                  (Boolean res, Boolean other) -> res && other);
    }
  }

  private Block allSignalled(final String smStateVar,
                             final String transitionVar,
                             final String newStateVar) {
    return Block
        .each(Ops.LOG_DEBUG, LOGGER, "Check allSignalled")
        .localSelect("$$smProgress", Path.stay()).out("*allProgress")
        .localSelect("$$smSignal", Path.stay()).out("*allSignals")
        .each(Ops.LOG_DEBUG, LOGGER,
              new Expr(Ops.TO_STRING, "all progress: ", "*allProgress"))
        .each(Ops.LOG_DEBUG, LOGGER,
              new Expr(Ops.TO_STRING, "all signals: ", "*allSignals"))
        .each(
          (RamaFunction2<StateConfig.OnAllSignalled<State, Signal>,
           APersistentMap,
           Boolean>)
          StateMachine::<State, Signal>isAllSignalled,
          transitionVar, "*allSignals").out("*isAllSignalled")
        .ifTrue(
          "*isAllSignalled",
          Block
          .each(Transition<State>::targetState,
                transitionVar).out(newStateVar)
          .each(Ops.LOG_DEBUG, LOGGER,
                new Expr(Ops.TO_STRING, "duratuion expired: ", newStateVar)),
          Block
          .each(
            (RamaFunction2<StateConfig.OnAllSignalled<State, Signal>,
             APersistentMap,
             Boolean>)
            StateMachine::<State, Signal>isAnyTimedOut,
            transitionVar, "*allProgress").out("*isAnyTimedOut")
          .ifTrue(
            "*isAnyTimedOut",
            Block.each(Ops.IDENTITY, null).out(newStateVar),
            Block.each(Ops.IDENTITY, null).out(newStateVar)));
  }

  public Block madeProgress(final String taskIdVar,
                            final String stateValueVar,
                            final String statusVar) {
    return Block
        .each(Ops.LOG_DEBUG, LOGGER,
              new Expr(Ops.TO_STRING,
                       "madeProgress task: ", taskIdVar,
                       ", state: ", stateValueVar,
                       ", status: ", statusVar))
        .each((RamaFunction3<Integer,
               State,
               PartitionProgress.PartitionStatus,
               PartitionProgress<State>>)
              PartitionProgress::<State>mkPartitionProgress,
            taskIdVar, stateValueVar, statusVar).out("*progress")
        .each(Ops.LOG_DEBUG, LOGGER,
              new Expr(Ops.TO_STRING,
                       "madeProgress progress: ", "*progress"))
        .depotPartitionAppend("*smCoordDepot", "*progress")
        .each(Ops.LOG_DEBUG, LOGGER, "madeProgress done");
  }

  public Block setSignal(final String taskIdVar, final String signalVar) {
    return Block
        .each((RamaFunction2<Integer, Signal, SignalUpdate<Signal>>)
              SignalUpdate::<Signal>mkSignalUpdate,
              taskIdVar,
              signalVar).out("*update")
        .depotPartitionAppend("*smCoordDepot", "*update");
  }
}
