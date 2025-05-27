package com.rpl.rama.helpers.statemachine.coordination;

import com.rpl.rama.RamaSerializable;

public class SignalUpdate <Signal extends Enum<Signal>>
    implements RamaSerializable {

  public int taskId;
  public Signal signal;

  public SignalUpdate(int taskId, Signal signal) {
    this.taskId = taskId;
    this.signal = signal;
  }

  public static <Signal extends Enum<Signal>>
      SignalUpdate<Signal> mkSignalUpdate(Integer taskId, Signal signal) {
    return new SignalUpdate<Signal>(taskId, signal);
  }

  public String toString() {
    return "SignalUpdate[taskId=" + taskId
        + ", signal=" + signal
        + "]";
  }

}
