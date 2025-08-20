package com.rpl.rama.helpers;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.ops.*;

/**
 * Higher-level PState implementation for a data structure from a key to a linked set of items. The underlying indexes
 * for the inner sets can be queried by set membership or by order of insertion.
 * <br><br>
 * This helper generates two underlying PStates, one of which can be queried for set membership queries and the other
 * can be queried for insertion order queries. One PState is called $$providedName and the other is called $$providedNameById.
 * <br><br>
 * The elements of the linked sets are referred to in this API as "entities". Each entity can be indexed by an ID within the
 * object. This class can be given an optional "entity id function" to extract those IDs from entities.
 * <br><br>
 * When inserted, an entity is given an "ID" which is either monotonically increasing or monotonically decreasing (this is
 * configurable). This ID is used to track order. A re-inserted entity is assigned a new ID. Order is preserved across keys.
 * <br><br>
 * The generated PStates store maps as the inner collections in order to achieve linked-set like operations. One PState is
 * a map from key to entityID to ID. This PState can be used for set membership type queries.
 * <br><br>
 * The other PState is a map from key to ID to entity. This can be used to perform ordering queries. Because the IDs are
 * monotonic, efficient pagination can be done using the <a href="https://beta.redplanetlabs.com/javadoc/com/rpl/rama/Path.html#sortedMapRange-java.lang.Object-java.lang.Object-">sortedMapRange</a>
 * and <a href="https://beta.redplanetlabs.com/javadoc/com/rpl/rama/Path.html#sortedMapRangeFrom-java.lang.Object-">sortedMapRangeFrom</a> navigators.
 * <br><br>
 * The pattern for this class is to create an instance and then use {@link declarePStates} to create all its underlying PStates
 * on the topology that should own it. This class provides further high-level operations that can be used with macros.
 * Queries are satisfied by querying the underlying PStates directly using paths.
 *
 * @see <a href="https://redplanetlabs.com/docs/~/pstates.html">PStates documentation</a>
 * @see <a href="https://redplanetlabs.com/docs/~/intermediate-dataflow.html#_macros">Detailed macro documentation</a>
 */
public class KeyToLinkedEntitySetPStateGroup {
  private final Class _keyType;
  private final Class _entityType;
  private Class _entityIdType;
  private final String _keyToIdToEntity;
  private final String _keyToEntityToId;
  private final ModuleUniqueIdPState _id;
  private RamaFunction1 _entityIdFunction = Ops.IDENTITY;

  /**
   * Creates instance of KeyToLinkedEntitySetPStateGroup. Methods on resulting object are used to declare PStates
   * and insert high-level operations into topology code.
   *
   * @param basePStateName Name used to derive PState names when `declarePStates` is called
   * @param keyType Type of keys
   * @param entityType type of entities within inner linked sets
   */
  public KeyToLinkedEntitySetPStateGroup(String basePStateName, Class keyType, Class entityType) {
    _keyType = keyType;
    _entityType = entityType;
    _entityIdType = entityType;
    _keyToEntityToId = basePStateName;
    _keyToIdToEntity = basePStateName + "ById";
    _id = new ModuleUniqueIdPState(basePStateName + "__idGen");
  }

  /**
   * Changes ID generation to insert entities in descending order instead of ascending order
   */
  public KeyToLinkedEntitySetPStateGroup descending() {
    _id.descending();
    return this;
  }

  /**
   * Provides a function to extract entity IDs from entities. If this is not specified, then entities
   * are their own entity IDs.
   *
   * @param entityIdType type of entity IDs
   * @param fn Function to extract entity ID from an entity at runtime
   */
  public KeyToLinkedEntitySetPStateGroup entityIdFunction(Class entityIdType, RamaFunction1 fn) {
    _entityIdFunction = fn;
    _entityIdType = entityIdType;
    return this;
  }

  /**
   * Declares needed PStates for this KeyToLinkedEntitySetPStateGroup on the specified topology. PStates
   * generated are $$basePStateName and $$basePStateNameById.
   */
  public void declarePStates(ETLTopologyBase topology) {
    _id.declarePState(topology);
    topology.pstate(
      _keyToEntityToId,
      PState.mapSchema(
        _keyType,
        PState.mapSchema(_entityIdType, Long.class).subindexed()));
    topology.pstate(
      _keyToIdToEntity,
      PState.mapSchema(
        _keyType,
        PState.mapSchema(Long.class, _entityType).subindexed()));
  }

  /**
   * Macro to add an entity to the linked set for a key
   *
   * @see <a href="https://redplanetlabs.com/docs/~/intermediate-dataflow.html#_macros">Detailed macro documentation</a>
   */
  public Block addToLinkedSet(Object key, Object entity) {
    String entityIdVar = Helpers.genVar("entityId");
    String currIdVar = Helpers.genVar("currId");
    String idVar = Helpers.genVar("id");
    return Block.each(_entityIdFunction, entity).out(entityIdVar)
                .localSelect(_keyToEntityToId, Path.key(key, entityIdVar)).out(currIdVar)
                // Here we want to remove the old id -> entity mapping because we're going to replace
                // the ID with a new one. We don't need to worry about the entity -> id mapping though
                // since we're going to overwrite that either way after the if
                .ifTrue(new Expr(Ops.IS_NOT_NULL, currIdVar),
                  Block.localTransform(_keyToIdToEntity, Path.key(key, currIdVar).termVoid()))
                .macro(_id.genId(idVar))
                .localTransform(_keyToEntityToId, Path.key(key, entityIdVar).termVal(idVar))
                .localTransform(_keyToIdToEntity, Path.key(key, idVar).termVal(entity));
  }

  /**
   * Macro to only continue processing if the specified key exists in the top-level map
   *
   * @see <a href="https://redplanetlabs.com/docs/~/intermediate-dataflow.html#_macros">Detailed macro documentation</a>
   */
  public Block filterKeyExists(Object key) {
    return Block.localSelect(_keyToEntityToId, Path.must(key));
  }

  /**
   * Macro to remove entity from inner linked set by its entity ID
   *
   * @see <a href="https://redplanetlabs.com/docs/~/intermediate-dataflow.html#_macros">Detailed macro documentation</a>
   */
  public Block removeFromLinkedSetByEntityId(Object key, Object entityId) {
    String idVar = Helpers.genVar("id");
    return Block.localSelect(_keyToEntityToId, Path.key(key).key(entityId)).out(idVar)
                .ifTrue(new Expr(Ops.IS_NOT_NULL, idVar),
                   Block.localTransform(_keyToEntityToId, Path.key(key).key(entityId).termVoid())
                        .localTransform(_keyToIdToEntity, Path.key(key).key(idVar).termVoid()));
  }

  /**
   * Macro to remove entity from inner linked set
   *
   * @see <a href="https://redplanetlabs.com/docs/~/intermediate-dataflow.html#_macros">Detailed macro documentation</a>
   */
  public Block removeFromLinkedSet(Object key, Object entity) {
    String entityIdVar = Helpers.genVar("entityId");
    return Block.each(_entityIdFunction, entity).out(entityIdVar)
                .macro(removeFromLinkedSetByEntityId(key, entityIdVar));
  }

  /**
   * Macro to remove key and its associated linked set entirely. Removes all state stored in both generated
   * PStates.
   *
   * @see <a href="https://redplanetlabs.com/docs/~/intermediate-dataflow.html#_macros">Detailed macro documentation</a>
   */
  public Block removeKey(Object key) {
    return Block.localTransform(_keyToEntityToId, Path.key(key).termVoid())
                .localTransform(_keyToIdToEntity, Path.key(key).termVoid());
  }
}
