package com.rpl.rama.helpers;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.ops.Ops;

/**
 * Higher-level PState helper for generating 4 or 8 byte IDs unique on a given task. IDs will not be unique across tasks.
 *
 * @see <a href="https://beta.redplanetlabs.com/docs/docs/1.0.0/pstates.html">PStates documentation</a>
 * @see <a href="https://beta.redplanetlabs.com/docs/docs/1.0.0/intermediate-dataflow.html#_macros">Detailed macro documentation</a>
 */
public class TaskUniqueIdPState {
  private final String _pstateName;
  private boolean _descending;
  private boolean _long;

  /**
   * Creates instance of TaskUniqueIdPState. Methods on resulting object are used to declare PState
   * and insert high-level operations into topology code. Defaults to generating 8 byte IDs.
   *
   * @param pstateName Name of resulting PState when `declarePState` is called
   */
  public TaskUniqueIdPState(String pstateName) {
    _pstateName = pstateName;
    _descending = false;
    _long = true;
  }

  /**
   * Change ID generation to create descending values per task. Note that there's no connection in ordering
   * for IDs from different tasks.
   */
  public TaskUniqueIdPState descending() {
    _descending = true;
    return this;
  }

  /**
   * Change ID generation to generate 4 byte IDs instead of 8 byte IDS.
   */
  public TaskUniqueIdPState integerIds() {
    _long = false;
    return this;
  }

  /**
   * Declare PState for this helper in the specified topology
   */
  public void declarePState(ETLTopologyBase topology) {
    Object init;
    Class schema;
    if(_long) {
      schema = Long.class;
      if(_descending) {
        init = Long.MAX_VALUE;
      } else {
        init = 0L;
      }
    } else {
      schema = Integer.class;
      if(_descending) {
        init = Integer.MAX_VALUE;
      } else {
        init = 0L;
      }
    }
    topology.pstate(_pstateName, schema).initialValue(init).makePrivate();
  }

  /**
   * Macro to generate a new ID on the given task
   *
   * @param outVar Var to bind the output
   * @see <a href="https://beta.redplanetlabs.com/docs/docs/1.0.0/intermediate-dataflow.html#_macros">Detailed macro documentation</a>
   */
  public Block genId(String outVar) {
    int change = _descending ? -1 : 1;
    return Block.each(Ops.EXTRACT_VALUE, _pstateName).out(outVar)
                .localTransform(_pstateName, Path.term(Ops.PLUS_LONG, change));
  }
}
