package com.rpl.rama.helpers.statemachine.core;

import com.rpl.rama.RamaSerializable;

import java.time.Duration;
import java.time.Instant;

public class StateMachineState<State extends Enum<State>>
    implements RamaSerializable {
  public State currentState;
  public Instant stateEnteredAt;

  public StateMachineState(State state) {
    currentState = state;
    stateEnteredAt = Instant.now();
  }

  public StateMachineState<State> setState(State state) {
    currentState = state;
    stateEnteredAt = Instant.now();
    return this;
  }

  State getCurrentState() {
    return currentState;
  }

  Duration elapsedDuration() {
    return Duration.between(stateEnteredAt, Instant.now());
  }
}
