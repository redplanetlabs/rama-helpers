package com.rpl.rama.helpers.statemachine.core;

import com.rpl.rama.RamaSerializable;

public class Transition<State extends Enum<State>> implements RamaSerializable {

  public State transitionTo;

  public TransitionType type() { return null; }

  public State targetState() {
    return transitionTo;
  }

  public String toString() {
    return "Transition: targetState " + transitionTo.toString();
  }
}
