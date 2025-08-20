package com.rpl.rama.helpers;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.ops.Ops;

/**
 * Higher-level PState helper for generating 8 byte IDs unique across the module. ID consists of 22 bits
 * for the generating task ID and 42 bits for a monotonically increasing or decreasing number on the task.
 *
 * @see <a href="https://redplanetlabs.com/docs/~/pstates.html">PStates documentation</a>
 * @see <a href="https://redplanetlabs.com/docs/~/intermediate-dataflow.html#_macros">Detailed macro documentation</a>
 */
public class ModuleUniqueIdPState {
  private final String _pstateName;
  private boolean _descending;

  /**
   * Creates instance of ModuleUniqueIdPState. Methods on resulting object are used to declare PState
   * and insert high-level operations into topology code.
   *
   * @param pstateName Name of resulting PState when `declarePState` is called
   */
  public ModuleUniqueIdPState(String pstateName) {
    _pstateName = pstateName;
    _descending = false;
  }

  /**
   * Change ID generation to create descending values per task. Note that there's no connection in ordering
   * for IDs from different tasks.
   */
  public ModuleUniqueIdPState descending() {
    _descending = true;
    return this;
  }

  /**
   * Declare PState for this helper in the specified topology
   */
  public void declarePState(ETLTopologyBase topology) {
    long init = _descending ? ((long) Math.pow(2, 42)) - 1 : 0;
    topology.pstate(_pstateName, Long.class).initialValue(init).makePrivate();
  }

  private static long generateId(Long id1, Integer taskId) {
    return (((long) taskId) << 42) | id1;
  }

  /**
   * Macro to generate a new unique ID on the given task
   *
   * @param outVar Var to bind the output
   * @see <a href="https://redplanetlabs.com/docs/~/intermediate-dataflow.html#_macros">Detailed macro documentation</a>
   */
  public Block genId(String outVar) {
    int change = _descending ? -1 : 1;
    String id1Var = Helpers.genVar("id1");
    String taskIdVar = Helpers.genVar("taskId");
    return Block.each(Ops.EXTRACT_VALUE, _pstateName).out(id1Var)
                .localTransform(_pstateName, Path.term(Ops.PLUS_LONG, change))
                .each(Ops.CURRENT_TASK_ID).out(taskIdVar)
                .each(ModuleUniqueIdPState::generateId, id1Var, taskIdVar).out(outVar);
  }
}
