package com.rpl.rama.helpers.spaatial.loadtest;

import java.time.Duration;

import com.rpl.rama.helpers.statemachine.core.StateMachine;
import com.rpl.rama.helpers.statemachine.core.StateMachineConfig;

public class LoadTestStateMachine {
  public enum LoadTestState {
    LOAD_DATA, TIME_PROCESSING, QUERY_PERFORMANCE
  }

  public enum LoadTestSignal {
    LOAD_COMPLETE
  }

  public StateMachine<LoadTestState, LoadTestSignal> stateMachine =
      new StateMachineConfig.Builder<LoadTestState, LoadTestSignal>()
      .state(LoadTestState.LOAD_DATA)
      .onAllSignalled(LoadTestSignal.LOAD_COMPLETE,
                      Duration.ofSeconds(3),
                      LoadTestState.TIME_PROCESSING )
      .done()
      .build()
      ;
}
