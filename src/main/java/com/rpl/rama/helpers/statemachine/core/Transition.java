package com.rpl.rama.helpers.statemachine.core;

public class Transition<State extends Enum<State>> {

  public State transitionTo;

  public TransitionType type() { return null; }

  public State targetState() {
    return transitionTo;
  }
}
