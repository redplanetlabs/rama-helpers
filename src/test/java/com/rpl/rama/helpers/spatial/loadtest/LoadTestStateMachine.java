package com.rpl.rama.helpers.spatial.loadtest;

import java.time.Duration;

import com.rpl.rama.RamaSerializable;
import com.rpl.rama.helpers.statemachine.core.StateMachine;
import com.rpl.rama.helpers.statemachine.core.StateMachineConfig;

public class LoadTestStateMachine implements RamaSerializable {
  public enum LoadTestState {
    DISABLE_MB, LOAD_DATA, ENABLE_MB, TIME_PROCESSING, QUERY_PERFORMANCE, DONE
  }

  public enum LoadTestSignal implements RamaSerializable {
    LOAD_COMPLETE, PROCESSING_COMPLETE
  }

  public StateMachine<LoadTestState, LoadTestSignal> stateMachine =
      new StateMachine<LoadTestState, LoadTestSignal>(
        new StateMachineConfig.Builder<LoadTestState, LoadTestSignal>()
        .state(LoadTestState.DISABLE_MB)
        .done()

        .state(LoadTestState.LOAD_DATA)
        .afterDuration(Duration.ofSeconds(10*60), LoadTestState.ENABLE_MB)
        // .onAllSignalled(LoadTestSignal.LOAD_COMPLETE,
        //                 Duration.ofSeconds(3),
        //                 LoadTestState.ENABLE_MB )
        .done()

        .state(LoadTestState.ENABLE_MB)
        .afterDuration(Duration.ofSeconds(10), LoadTestState.TIME_PROCESSING)
        .done()

        .state(LoadTestState.TIME_PROCESSING)
        .onAllSignalled(LoadTestSignal.PROCESSING_COMPLETE,
                        Duration.ofSeconds(3),
                        LoadTestState.TIME_PROCESSING )
        .done()

        .state(LoadTestState.QUERY_PERFORMANCE)
        .afterDuration(Duration.ofSeconds(3), LoadTestState.DONE)
        .done()

        .state(LoadTestState.DONE)
        .done()

        .build())
      ;
}
