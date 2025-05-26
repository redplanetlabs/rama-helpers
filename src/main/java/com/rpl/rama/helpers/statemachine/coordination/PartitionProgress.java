package com.rpl.rama.helpers.statemachine.coordination;

public class PartitionProgress <State extends Enum<State>>  {
  public enum PartitionStatus {
    READY, WORKING, COMPLETE, FAILED, TIMEOUT
  }

  public long partitionId;
  public long timestamp;
  public State state;
  public PartitionStatus status;
}
