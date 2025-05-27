package com.rpl.rama.helpers.statemachine.core;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;

import com.rpl.rama.RamaSerializable;

public class StateConfig<State extends Enum<State>,
                               Signal extends Enum<Signal>>
    implements RamaSerializable {

  public static class OnTimeout<State extends Enum<State>>
      extends Transition<State> implements RamaSerializable {
    public Duration timeoutDuration;
    public boolean requiresProgress;
    public TransitionType type() { return TransitionType.TIMEOUT; }
  }

  public static class AfterDuration<State extends Enum<State>>
      extends Transition<State> implements RamaSerializable {
    public Duration duration;
    public TransitionType type() { return TransitionType.DURATION; }

    public Boolean isExpired(final Duration elapsed) {
      return elapsed.compareTo(duration) > 0;
    }

    public String toString() {
      return "AfterDuration: targetState " +
          (transitionTo == null ? "null" : transitionTo.toString())
          + ", duration: " + duration.toString();
    }
  }

  public static class OnAllSignalled<State extends Enum<State>,
                                           Signal extends Enum<Signal>>
      extends Transition<State> implements RamaSerializable {
    public Signal signal;
    public Duration timeout;
    public TransitionType type() { return TransitionType.ALL_SIGNALLED; }
  }

  public List<Transition<State>> transitions;

  Iterator<Transition<State>> getTransitions() {
    return transitions.iterator();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Transitions: {\n");

    transitions.forEach((transition) -> {
        sb.append("    ").append(transition).append("\n");
      });

    sb.append("}");
    return sb.toString();
  }
}
