package com.rpl.rama.helpers;

import com.rpl.rama.*;
import com.rpl.rama.module.ETLTopologyBase;
import com.rpl.rama.ops.*;

import java.util.SortedMap;

/**
 * Higher-level PState implementation for a data structure from a key to a collection of items with a maximum cardinality.
 * Automatically drops oldest item on adding a new item when max cardinality is reached. Inner collection is a map
 * from id -> element, with ids being monotonically decreasing starting at Long.MAX_VALUE.
 * <br><br>
 * Declares three PStates underneath the hood. The provided PState name in the constructor should be used for all queries,
 * another one has "Reverse" appended to the name and can look up an internal ID for an entity, and the last one is
 * used for internal metadata as part of the implementation.
 * <br><br>
 * The pattern for this class is to create an instance and then use {@link declarePStates} to create all its needed PStates
 * on the topology that should own it. The other methods define high-level operations to perform on this data structure.
 *
 * @see <a href="https://beta.redplanetlabs.com/docs/docs/1.0.0/pstates.html">PStates documentation</a>
 * @see <a href="https://beta.redplanetlabs.com/docs/docs/1.0.0/intermediate-dataflow.html#_macros">Detailed macro documentation</a>
 */
public class KeyToUniqueFixedItemsPStateGroup {
  private final String _pstate;
  private final String _pstateReverse;
  private final String _meta;
  private final int _maxAmt;
  private final Class _keyClass;
  private final Class _itemClass;
  private RamaFunction1 _entityIdFunction;
  private Class _entityIdType;
  private int _clearBatchSize;

  /**
   * Creates instance of KeyToUniqueFixedItemsPStateGroup. Methods on resulting object are used to declare PStates
   * and insert high-level operations into topology code.
   *
   * @param pstateName Base name for created PStates. Creates one PState of this name to be used for querying, and another
   * PState with a derived name to store metadata.
   * @param maxAmt Maximum cardinality for inner collections
   * @param keyClass Type of keys in top-level map
   * @param itemClass Type of elements in inner collections
   */
  public KeyToUniqueFixedItemsPStateGroup(String pstateName, int maxAmt, Class keyClass, Class itemClass) {
    _pstate = pstateName;
    _pstateReverse = pstateName + "Reverse";
    _meta = pstateName + "Meta";
    _maxAmt = maxAmt;
    _keyClass = keyClass;
    _itemClass = itemClass;
    _clearBatchSize = 100;
  }

  /**
   * Configures how many items to clear at a time during execution of {@link #clearItems()}
   */
  public KeyToUniqueFixedItemsPStateGroup clearBatchSize(int size) {
    _clearBatchSize = size;
    return this;
  }


  public KeyToUniqueFixedItemsPStateGroup entityIdFunction(Class entityIdType, RamaFunction1 fn) {
    _entityIdFunction = fn;
    _entityIdType = entityIdType;
    return this;
  }

  /**
   * Declares needed PStates for this KeyToUniqueFixedItemsPStateGroup on the specified topology
   */
  public void declarePStates(ETLTopologyBase topology) {
    topology.pstate(
      _pstate,
      PState.mapSchema(
        _keyClass,
        PState.mapSchema(Long.class, _itemClass).subindexed(SubindexOptions.withoutSizeTracking())
        ));
    topology.pstate(
      _pstateReverse,
      PState.mapSchema(
        _keyClass,
        PState.mapSchema(_entityIdType == null ? _itemClass : _entityIdType, Long.class).subindexed(SubindexOptions.withoutSizeTracking())
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
    String dropItemVar = Helpers.genVar("dropItem");
    String newMetaVar = Helpers.genVar("newMeta");
    String dropIdAndItemExtraVar = Helpers.genVar("dropIdAndItemExtra");
    String dropIdExtraVar = Helpers.genVar("dropIdExtra");
    String dropItemExtraVar = Helpers.genVar("dropItemExtra");
    String entityIdVar = Helpers.genVar("entityId");
    String dropEntityIdVar = Helpers.genVar("dropEntityId");
    return Block.macro(removeItem(key, item))
                .localSelect(_meta, Path.key(key)).out(metaVar)
                .ifTrue(new Expr(Ops.IS_NULL, metaVar),
                   Block.each(Ops.IDENTITY, Long.MAX_VALUE).out(idVar)
                        .each(Ops.IDENTITY, _maxAmt).out(maxAmtVar),
                   Block.each(Ops.GET, metaVar, 0).out(idVar)
                        .each(Ops.GET, metaVar, 1).out(maxAmtVar))
                .each(KeyToUniqueFixedItemsPStateGroup::computeDropId, idVar, _maxAmt).out(dropIdVar)
                .localSelect(_pstate, Path.key(key, dropIdVar)).out(dropItemVar)
                .macro(extractEntityId(dropItemVar, dropEntityIdVar))
                .each(Ops.TUPLE, new Expr(Ops.DEC_LONG, idVar), _maxAmt).out(newMetaVar)
                .localTransform(_meta, Path.key(key).termVal(newMetaVar))
                .macro(extractEntityId(item, entityIdVar))
                .localTransform(_pstate,
                                Path.key(key)
                                    .multiPath(Path.key(idVar).termVal(item),
                                               Path.key(dropIdVar).termVoid()))
                .localTransform(_pstateReverse,
                                Path.key(key)
                                    .multiPath(Path.key(entityIdVar).termVal(idVar),
                                               Path.key(dropEntityIdVar).termVoid()))
                .ifTrue(new Expr(Ops.NOT_EQUAL, maxAmtVar, _maxAmt),
                   Block.localSelect(_pstate, Path.key(key).sortedMapRangeFrom(dropIdVar).all()).out(dropIdAndItemExtraVar)
                        .each(Ops.EXPAND, dropIdAndItemExtraVar).out(dropIdExtraVar, dropItemExtraVar)
                        .localTransform(_pstate, Path.key(key, dropIdExtraVar).termVoid())
                        .localTransform(_pstateReverse, Path.key(key, dropItemExtraVar).termVoid()));
  }

  private Block extractEntityId(Object item, String entityIdVar) {
    return Block.each(_entityIdFunction == null ? Ops.IDENTITY : _entityIdFunction, item).out(entityIdVar);
  }

  /**
   * Macro to remove item from collection for specified key
   *
   * @see <a href="https://beta.redplanetlabs.com/docs/docs/1.0.0/intermediate-dataflow.html#_macros">Detailed macro documentation</a>
   */
  public Block removeItem(Object key, Object item) {
    String entityIdVar = Helpers.genVar("entityId");
    return Block.macro(extractEntityId(item, entityIdVar))
                .macro(removeItemByEntityId(key, entityIdVar));
  }


  /**
   * Macro to remove entity from inner fixed list by its entity ID
   *
   * @see <a href="https://beta.redplanetlabs.com/docs/docs/1.0.0/intermediate-dataflow.html#_macros">Detailed macro documentation</a>
   */
  public Block removeItemByEntityId(Object key, Object entityId) {
    String existingIdVar = Helpers.genVar("existingId");
    return Block.localSelect(_pstateReverse, Path.key(key, entityId)).out(existingIdVar)
                .ifTrue(new Expr(Ops.IS_NOT_NULL, existingIdVar),
                  Block.localTransform(_pstate, Path.key(key, existingIdVar).termVoid())
                       .localTransform(_pstateReverse, Path.key(key, entityId).termVoid()));
  }

  public Block removeItemById(Object key, Object id) {
    String existingItemVar = Helpers.genVar("existingItem");
    return Block.localSelect(_pstate, Path.key(key, id)).out(existingItemVar)
            .ifTrue(new Expr(Ops.IS_NOT_NULL, existingItemVar),
                    Block.localTransform(_pstate, Path.key(key, id).termVoid())
                         .localTransform(_pstateReverse, Path.key(key, existingItemVar).termVoid()));
  }

  /**
   * Macro to remove key and its underlying collection
   *
   * @see <a href="https://beta.redplanetlabs.com/docs/docs/1.0.0/intermediate-dataflow.html#_macros">Detailed macro documentation</a>
   */
  public Block removeKey(Object key) {
    return Block.localTransform(_meta, Path.key(key).termVoid())
                .localTransform(_pstate, Path.key(key).termVoid())
                .localTransform(_pstateReverse, Path.key(key).termVoid());
  }

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
}
