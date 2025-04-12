package com.rpl.rama.helpers.spatial;

import com.rpl.rama.Agg;
import com.rpl.rama.Block;
import com.rpl.rama.CompoundAgg;
import com.rpl.rama.PState;
import com.rpl.rama.Path;
import com.rpl.rama.RamaModule.Topologies;
import com.rpl.rama.helpers.ModuleUniqueIdPState;
import com.rpl.rama.module.ETLTopologyBase;

class RTree {
  private final String pstatePrefix;
  private final int dimensions;
  private final Class<?> objectType;
  // private final ModuleUniqueIdPState id;

  public RTree(final int dimensions, final String pstatePrefix, final Class<?> objectType) {
    this.pstatePrefix = pstatePrefix;
    this.dimensions = dimensions;
    this.objectType = objectType;
    // this.id = new ModuleUniqueIdPState(pstatePrefix + "__rtree");
  }

  public void declarePStates(final ETLTopologyBase topology) {
    // id.declarePState(topology);
    topology.pstate(pstatePrefix,
		    PState.mapSchema(MBR.class, objectType));
  }

  public void declareQueries(final Topologies topologies) {
    topologies.query("objectsInBounds", "*bounds").out("*objects")
      .hashPartition("*bounds")
      .localSelect(pstatePrefix, Path.key("*bounds")).out("*objects")
      .originPartition()
      .agg(Agg.list("*objects")).out("*objects");
  }

  public Block addObject(final String boundsVar, final String objectVar) {
    return Block.localTransform(pstatePrefix, Path.key(boundsVar).termVal(objectVar));
  }
}
