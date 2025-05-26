package com.rpl.rama.helpers.statemachine.core;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.rpl.rama.RamaSerializable;

public class StateMachineConfig<State extends Enum<State>,
                                      Signal extends Enum<Signal>>
    implements RamaSerializable {

  private Map<State, StateConfig<State, Signal>> stateConfigs;

  public StateConfig<State, Signal> stateConfig(State state) {
    return stateConfigs.get(state);
  }

  private StateMachineConfig(Map<State, StateConfig<State, Signal>> stateConfigs) {
    this.stateConfigs = stateConfigs;
  }

  public static class Builder<State extends Enum<State>,
                                    Signal extends Enum<Signal>> {

    private Map<State, StateConfig<State, Signal>> stateConfigs = new HashMap<>();

    public StateConfigBuilder<State, Signal> state(State state) {
      return new StateConfigBuilder<>(this, state);
    }

    public StateMachineConfig<State, Signal> build() {
      return new StateMachineConfig<State, Signal>(new HashMap<>(stateConfigs));
    }

    private void addStateConfig(State state, StateConfig<State, Signal> config) {
      stateConfigs.put(state, config);
    }
  }

  public static class StateConfigBuilder<State extends Enum<State>,
                                               Signal extends Enum<Signal>> {

    private final Builder<State, Signal> parent;
    private final State state;
    private final List<Transition<State>> transitions = new ArrayList<>();

    StateConfigBuilder(Builder<State, Signal> parent, State state) {
      this.parent = parent;
      this.state = state;
    }

    public StateConfigBuilder<State, Signal> onTimeout(Duration timeoutDuration,
        boolean requiresProgress,
        State targetState) {
      StateConfig.OnTimeout<State> timeout = new StateConfig.OnTimeout<>();
      timeout.timeoutDuration = timeoutDuration;
      timeout.requiresProgress = requiresProgress;
      timeout.transitionTo = targetState;
      transitions.add(timeout);
      return this;
    }

    public StateConfigBuilder<State, Signal> afterDuration(Duration duration,
        State targetState) {
      StateConfig.AfterDuration<State> afterDuration
          = new StateConfig.AfterDuration<>();
      afterDuration.duration = duration;
      afterDuration.transitionTo = targetState;
      transitions.add(afterDuration);
      return this;
    }

    public StateConfigBuilder<State, Signal> onAllSignalled(Signal signal,
        State targetState) {
      StateConfig.OnAllSignalled<State, Signal> onAllSignalled
          = new StateConfig.OnAllSignalled<>();
      onAllSignalled.signal = signal;
      onAllSignalled.transitionTo  = targetState;
      transitions.add(onAllSignalled);
      return this;
    }

    public Builder<State, Signal> done() {
      StateConfig<State, Signal> config = new StateConfig<>();
      config.transitions = new ArrayList<>(transitions);
      parent.addStateConfig(state, config);
      return parent;
    }
  }
}
