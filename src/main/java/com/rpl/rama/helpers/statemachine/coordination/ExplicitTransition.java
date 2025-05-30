package com.rpl.rama.helpers.statemachine.coordination;

import com.rpl.rama.RamaSerializable;

public class ExplicitTransition<State extends Enum<State>>
    implements RamaSerializable {
  public State state;

  public ExplicitTransition(State state) {
    this.state = state;
  }

  public static <State extends Enum<State>>
  ExplicitTransition<State> mkExplicitTransition(State state) {
    return new ExplicitTransition<State>(state);
  }

  public String toString() {
    return "ExplicitTransition[state=" + state + "]";
  }
}
