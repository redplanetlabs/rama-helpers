package com.rpl.rama.helpers.statemachine.coordination;

import com.rpl.rama.RamaSerializable;

public class PartitionProgress <State extends Enum<State>>
    implements RamaSerializable {

  public enum PartitionStatus implements RamaSerializable {
    READY, WORKING, COMPLETE, FAILED, TIMEOUT
  }

  public int taskId;
  public long timestamp;
  public State state;
  public PartitionStatus status;

  public PartitionProgress(int taskId,
                           State state,
                           PartitionStatus status) {
    this.taskId = taskId;
    this.timestamp = System.currentTimeMillis();
    this.state = state;
    this.status = status;
  }

  public static <State extends Enum<State>>
  PartitionProgress<State> mkPartitionProgress(Integer taskId,
                                               State state,
                                               PartitionStatus status) {
    return new PartitionProgress<State>(taskId, state, status);
  }

  public String toString() {
    return "PartitionState[taskId=" + taskId
        + ", state=" + state
        + ", status=" + status
        + "]";
  }

}
