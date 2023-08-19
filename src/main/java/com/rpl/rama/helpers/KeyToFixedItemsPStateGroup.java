package com.rpl.rama.helpers;

import com.rpl.rama.*;
import com.rpl.rama.module.ETLTopologyBase;
import com.rpl.rama.ops.*;

import java.util.*;

/**
 * Higher-level PState implementation for a data structure from a key to a collection of items with a maximum cardinality.
 * Automatically drops oldest item on adding a new item when max cardinality is reached. Inner collection is a map
 * from id to element, with ids being monotonically increasing decreasing starting at Long.MAX_VALUE.
 * <br><br>
 * Declares two PStates underneath the hood. The provided PState name in the constructor should be used for all queries,
 * while the second one is used for internal metadata as part of the implementation.
 * <br><br>
 * The pattern for this class is to create an instance and then use {@link declarePStates} to create all its needed PStates
 * on the topology that should own it. The other methods define high-level operations to perform on this data structure.
 * <br><br>
 * @see <a href="https://beta.redplanetlabs.com/docs/docs/1.0.0/pstates.html">PStates documentation</a>
 * @see <a href="https://beta.redplanetlabs.com/docs/docs/1.0.0/intermediate-dataflow.html#_macros">Detailed macro documentation</a>
 */
public class KeyToFixedItemsPStateGroup {
  private final String _pstate;
  private final String _meta;
  private final int _maxAmt;
  private final Class _keyClass;
  private final Class _itemClass;
  private int _clearBatchSize;

  /**
   * Creates instance of KeyToFixedItemsPStateGroup. Methods on resulting object are used to declare PStates
   * and insert high-level operations into topology code.
   *
   * @param pstateName Base name for created PStates. Creates one PState of this name to be used for querying, and another
   * PState with a derived name to store metadata.
   * @param maxAmt Maximum cardinality for inner collections
   * @param keyClass Type of keys in top-level map
   * @param itemClass Type of elements in inner collections
   */
  public KeyToFixedItemsPStateGroup(String pstateName, int maxAmt, Class keyClass, Class itemClass) {
    _pstate = pstateName;
    _meta = pstateName + "Meta";
    _maxAmt = maxAmt;
    _keyClass = keyClass;
    _itemClass = itemClass;
    _clearBatchSize = 100;
  }

  /**
   * Configures how many items to clear at a time during execution of {@link clearItems(Object)}
   */
  public KeyToFixedItemsPStateGroup clearBatchSize(int size) {
    _clearBatchSize = size;
    return this;
  }

  /**
   * Declares needed PStates for this KeyToFixedItemsPStateGroup on the specified topology
   */
  public void declarePStates(ETLTopologyBase topology) {
    topology.pstate(
      _pstate,
      PState.mapSchema(
        _keyClass,
        PState.mapSchema(Long.class, _itemClass).subindexed(SubindexOptions.withoutSizeTracking())
        ));
    topology.pstate(
      _meta,
      PState.mapSchema(_keyClass, Object.class)
      );
  }

  private static Long computeDropId(Long id, Integer maxAmt) {
    if(id <= Long.MAX_VALUE - maxAmt) {
      return id + maxAmt;
    } else {
      return null;
    }
  }

  /**
   * Macro to add item to collection for specified key
   *
   * @see <a href="https://beta.redplanetlabs.com/docs/docs/1.0.0/intermediate-dataflow.html#_macros">Detailed macro documentation</a>
   */
  public Block addItem(Object key, Object item) {
    String metaVar = Helpers.genVar("meta");
    String idVar = Helpers.genVar("id");
    String maxAmtVar = Helpers.genVar("maxAmt");
    String dropIdVar = Helpers.genVar("dropId");
    String newMetaVar = Helpers.genVar("newMeta");
    return Block.localSelect(_meta, Path.key(key)).out(metaVar)
                .ifTrue(new Expr(Ops.IS_NULL, metaVar),
                  Block.each(Ops.IDENTITY, Long.MAX_VALUE).out(idVar)
                       .each(Ops.IDENTITY, _maxAmt).out(maxAmtVar),
                  Block.each(Ops.GET, metaVar, 0).out(idVar)
                       .each(Ops.GET, metaVar, 1).out(maxAmtVar))
                .each(KeyToFixedItemsPStateGroup::computeDropId, idVar, _maxAmt).out(dropIdVar)
                .each(Ops.TUPLE, new Expr(Ops.DEC_LONG, idVar), _maxAmt).out(newMetaVar)
                .localTransform(_meta, Path.key(key).termVal(newMetaVar))
                .localTransform(
                  _pstate,
                  Path.key(key).multiPath(
                    Path.key(idVar).termVal(item),
                    Path.key(dropIdVar).termVoid()))
                .ifTrue(new Expr(Ops.NOT_EQUAL, maxAmtVar, _maxAmt),
                  Block.localTransform(
                    _pstate,
                    Path.key(key).sortedMapRangeFrom(dropIdVar).mapKeys().termVoid()
                    ));
  }

  /**
   * Macro to remove an item by its ID. No-op if the ID doesn't exist.
   *
   * @see <a href="https://beta.redplanetlabs.com/docs/docs/1.0.0/intermediate-dataflow.html#_macros">Detailed macro documentation</a>
   */
  public Block removeItemById(Object key, Object id) {
    return Block.localTransform(_pstate, Path.key(key, id).termVoid());
  }

  /**
   * Macro to clear current items for a key. The clear is done piecemeal so as not to dominate the task thread. Only items
   * that exist at the start are cleared. New items that come in during the clear will remain.
   *
   * @see <a href="https://beta.redplanetlabs.com/docs/docs/1.0.0/intermediate-dataflow.html#_macros">Detailed macro documentation</a>
   */
  public Block clearItems(Object key) {
    return Block.loopWithVars(LoopVars.var("*i", -1L),
             Block.yieldIfOvertime()
             	  .localSelect(_pstate,
             			  	   Path.key(key)
             			  	       .sortedMapRangeFrom(
             			  	    	  "*i", 
             			  	    	  SortedRangeFromOptions.excludeStart().maxAmt(_clearBatchSize))).out("*m")
             	  .atomicBlock(
             		 Block.each(Ops.EXPLODE_MAP, "*m").out("*i", "*v")
             		 	  .macro(removeItemById(key, "*i")))
             	  .ifTrue(new Expr(Ops.LESS_THAN, new Expr(Ops.SIZE, "*m"), _clearBatchSize),
             			  Block.emitLoop(),
             			  Block.each((SortedMap m) -> m.lastKey(), "*m").out("*nextI")
             			  	   .continueLoop("*i")));
  }

  /**
   * Macro to remove key and its underlying collection
   *
   * @see <a href="https://beta.redplanetlabs.com/docs/docs/1.0.0/intermediate-dataflow.html#_macros">Detailed macro documentation</a>
   */
  public Block removeKey(Object key) {
    return Block.localTransform(_meta, Path.key(key).termVoid())
                .localTransform(_pstate, Path.key(key).termVoid());
  }
}
