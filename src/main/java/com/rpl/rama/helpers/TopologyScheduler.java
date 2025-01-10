package com.rpl.rama.helpers;

import java.util.*;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.ops.*;

/**
 * Utility for scheduling future work at specific times within a topology. Both scheduling and processing
 * must be done by the same topology.
 * <br><br>
 * The pattern for this class is to create an instance and then use {@link declarePStates} to create all its needed PStates
 * on the topology that should own it. The operation {@link scheduleItem} is used to schedule an item for processing in the future,
 * and the operation `handleExpirations` is used to process items whose timers have expired.
 * <br><br>
 * It is up to the user of this class to determine when {@link handleExpirations} is called. A common way to use it is to invoke
 * it off of a tick depot.
 */
public class TopologyScheduler {
  private static final String MAX_UUID = "ffffffff-ffff-ffff-ffff-ffffffffffff";

  String _pstateVar;
  ETLTopologyBase _owningTopology;
  int _maxFetchAmt = 1000;

  /**
   * Creates an instance of TopologyScheduler.
   *
   * @param pstatePrefix Prefix to use for all PStates created by this instance. Must begin with "$$".
   */
  public TopologyScheduler(String pstatePrefix) {
    _pstateVar = pstatePrefix + "Expirations";
  }

  /**
   * Configures the maximum number of items to process whenever `handleExpirations` is invoked. Defaults to 1000.
   */
  public TopologyScheduler maxFetchAmt(int amt) {
    _maxFetchAmt = amt;
    return this;
  }

  private static String padTimeStr(Long timestampMillis) {
    return String.format("%014d", timestampMillis);
  }

  /**
   * Declares all needed PStates for this instance.
   */
  public void declarePStates(ETLTopologyBase topology) {
    if(_owningTopology!=null) throw new RuntimeException("May not declare TopologyScheduler multiple times");
    _owningTopology = topology;
    topology.pstate(_pstateVar, PState.mapSchema(List.class, Object.class));
  }

  /**
   * Macro to insert code to check for expired items and process them. The generated code goes to all tasks with
   * {@link Block#allPartition()}, and items are processed on the same task on which they were scheduled. The way expired
   * items can be processed differs between streaming and microbatching.
   * <br><br>
   *
   * With used in a streaming topology, code attached after the provided handleCode should execute exactly one time and only execute
   * when all needed computation for processing an item has completed (e.g. PState updates).
   *
   * @param itemVar Var to bind the item to be processed. This var will be in scope for handleCode, and its value was provided when {@link scheduleItem} was called.
   * @param currentTimeVar Var to bind the timestamp used to fetch expired items. This var will be in scope for `handleCode`.
   * @param handleCode Block of code to process items.
   */
  public Block.Impl handleExpirations(String itemVar, String currentTimeVar, Block.Impl handleCode) {
    String targetVar = Helpers.genVar("targetTuple");
    String mvar = Helpers.genVar("map");
    String itVar = Helpers.genVar("iterator");
    String actionVar = Helpers.genVar("action");
    String entryVar = Helpers.genVar("entry");
    String keyVar = Helpers.genVar("key");
    SortedRangeToOptions options = SortedRangeToOptions.includeEnd().maxAmt(_maxFetchAmt);
    Block.Impl start = Block.each(TopologyUtils::currentTimeMillis).out(currentTimeVar)
                            .allPartition()
                            .each(Ops.TUPLE, new Expr(TopologyScheduler::padTimeStr, currentTimeVar), MAX_UUID).out(targetVar)
                            .localSelect(_pstateVar, Path.sortedMapRangeTo(targetVar, options)).out(mvar)
                            .each((Map m) -> m.entrySet().iterator(), mvar).out(itVar)
                            .loop(
                              Block.yieldIfOvertime()
                                   .ifTrue(new Expr((RamaFunction1<Iterator, Boolean>) Iterator::hasNext, itVar),
                                     Block.each((RamaFunction1<Iterator, Object>) Iterator::next, itVar).out(entryVar)
                                          .each((Map.Entry e) -> e.getKey(), entryVar).out(keyVar)
                                          .each((Map.Entry e) -> e.getValue(), entryVar).out(itemVar)
                                          .emitLoop("u", keyVar, itemVar)
                                          .continueLoop(),
                                     Block.emitLoop(null, null, null))).out(actionVar, keyVar, itemVar);
    if(_owningTopology instanceof MicrobatchTopology) {
      String doneAnchor = Helpers.genVar("Done").substring(1);
      return start.ifTrue(new Expr(Ops.EQUAL, actionVar, "u"),
                    Block.macro(handleCode),
                    Block.localTransform(_pstateVar, Path.sortedMapRangeTo(targetVar, options).termVal(null))
                         .anchor(doneAnchor))
                  .hook(doneAnchor);
    } else if(_owningTopology instanceof StreamTopology) {
      String originTaskVar = Helpers.genVar("originTask");
      return start.ifTrue(new Expr(Ops.EQUAL, actionVar, "u"),
                    Block.each(Ops.CURRENT_TASK_ID).out(originTaskVar)
                         .macro(handleCode)
                         .directPartition(originTaskVar)
                         .localTransform(_pstateVar, Path.key(keyVar).termVoid()));
    } else {
      throw new RuntimeException("Unknown topology type " + _owningTopology.getClass());
    }
  }

  /**
   * Macro to schedule an item for future processing. The item will be processed on the same task on which it was scheduled.
   *
   * @param timestampMillis Time at which to schedule processing
   * @param item Item to process
   */
  public Block.Impl scheduleItem(Object timestampMillis, Object item) {
    String uuidVar = Helpers.genVar("scheduledUUID");
    String tupleVar = Helpers.genVar("scheduleTuple");
    String longVar = Helpers.genVar("timestampLong");
    return Block.each(() -> UUID.randomUUID().toString()).out(uuidVar)
                .each((Number n) -> n.longValue(), timestampMillis).out(longVar)
                .each(Ops.TUPLE, new Expr(TopologyScheduler::padTimeStr, longVar), uuidVar).out(tupleVar)
                .localTransform(_pstateVar, Path.key(tupleVar).termVal(item));
  }

}
