package com.rpl.rama.helpers.statemachine.core;

import java.util.Iterator;

import com.rpl.rama.Block;
import com.rpl.rama.Case;
import com.rpl.rama.Expr;
import com.rpl.rama.Helpers;
import com.rpl.rama.Path;
import com.rpl.rama.RamaSerializable;
import com.rpl.rama.RamaModule.Setup;
import com.rpl.rama.RamaModule.Topologies;
import com.rpl.rama.module.MicrobatchTopology;
import com.rpl.rama.ops.Ops;
import com.rpl.rama.ops.RamaFunction1;

import org.slf4j.LoggerFactory;

public class StateMachine<State extends Enum<State>,
                                Signal extends Enum<Signal>>
    implements RamaSerializable {

  public static final Object LOGGER =
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
    setup.declareTickDepot("*smDepot", 100);  // 100ms

    MicrobatchTopology sm = topologies.microbatch("sm");
    StateMachineState<State> smState
        = new StateMachineState<State>(initState);
    sm.pstate("$$sm", StateMachineState.class).global().initialValue(smState);
    sm.pstate("$$smCoord", Object.class);

    final String smStateVar = Helpers.genVar("smState");
    final String newStateVar = Helpers.genVar("newState");
    sm.source("*smDepot").batchBlock(
      Block
      .each(Ops.LOG_DEBUG, LOGGER, "StateMachine coord")
      .localSelect("$$sm", Path.stay()).out(smStateVar)
      .each(Ops.LOG_DEBUG, LOGGER, "AA")
      .each(StateMachineState<State>::getCurrentState,
            smStateVar).out("*stateValue")
      .each(Ops.LOG_DEBUG, LOGGER, "BB")
      .each(StateMachine<State, Signal>::stateConfig, this,
            "*stateValue").out("*stateConfig")
      .each(Ops.LOG_DEBUG, LOGGER, "CC")
      .each(StateConfig<State, Signal>::getTransitions,
            "*stateConfig").out("*transitionIter")
      .each(Ops.LOG_DEBUG, LOGGER, "DD")
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
  }

  private Block nextState(final String smStateVar,
                          final String transitionVar,
                          final String newStateVar) {
    return Block
        .each((RamaFunction1<Transition<State>, TransitionType>)
              Transition::type, transitionVar).out("*type")
        .cond(
          Case
          .create(new Expr(Ops.EQUAL, TransitionType.TIMEOUT, "*type"))
          .macro(timeoutTransition(smStateVar, transitionVar, newStateVar)),
          Case
          .create(new Expr(Ops.EQUAL, TransitionType.DURATION, "*type"))
          .macro(duration(smStateVar, transitionVar, newStateVar)),
          Case
          .create(new Expr(Ops.EQUAL, TransitionType.DURATION, "*type"))
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
                      transitionVar).out("*unused")
                .each(Transition<State>::targetState,
                      transitionVar).out(newStateVar)
                .each(Ops.LOG_DEBUG, LOGGER,
              new Expr(Ops.TO_STRING, "duratuion expired: ", newStateVar)),
                Block.each(Ops.IDENTITY, null).out(newStateVar));
  }

  private Block allSignalled(final String smStateVar,
                             final String transitionVar,
                             final String newStateVar) {
    return Block.each(Ops.IDENTITY, null).out(newStateVar);
  }

}
