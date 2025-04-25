package com.rpl.rama.helpers.spatial;

import com.rpl.rama.Block;
import com.rpl.rama.helpers.ModuleUniqueIdPState;

public class TestModuleUniqueIdPState extends ModuleUniqueIdPState {
  private boolean _descending;
  private long value;

  /**
   * Creates instance of ModuleUniqueIdPState. Methods on resulting object are used to declare PState
   * and insert high-level operations into topology code.
   *
   * @param pstateName Name of resulting PState when `declarePState` is called
   */
  public TestModuleUniqueIdPState() {
    super("unused");
    _descending = false;
    value = 0;
  }

  /**
   * Change ID generation to create descending values per task. Note that there's no connection in ordering
   * for IDs from different tasks.
   */
  public TestModuleUniqueIdPState descending() {
    _descending = true;
    value = ((long) Math.pow(2, 42)) - 1;
    return this;
  }

  private static long generateId(Long id1, Integer taskId) {
    return (((long) 0) << 42) | id1;
  }

  public long nextValue() {
      value = _descending ? value - 1 : value + 1;
      return generateId(value, 0);
  }

  /**
   * Macro to generate a new unique ID on the given task
   *
   * @param outVar Var to bind the output
   */
  public Block genId(String outVar) {
    return Block
           .each(TestModuleUniqueIdPState::nextValue, this)
           .out(outVar);
  }
}
