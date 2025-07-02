package com.rpl.rama.helpers.spatial;

import clojure.lang.PersistentTreeMap;
import clojure.lang.PersistentVector;
import clojure.lang.Counted;
import clojure.lang.LazySeq;
import clojure.lang.PersistentArrayMap;
import clojure.lang.PersistentHashMap;

import com.rpl.rama.Agg;
import com.rpl.rama.Block;
import com.rpl.rama.CompoundAgg;
import com.rpl.rama.Expr;
import com.rpl.rama.Helpers;
import com.rpl.rama.LoopVars;
import com.rpl.rama.ModuleInstanceInfo;
import com.rpl.rama.PState;
import com.rpl.rama.Path;
import com.rpl.rama.RamaSerializable;
import com.rpl.rama.SubBatch;
import com.rpl.rama.RamaModule.Topologies;
import com.rpl.rama.helpers.ModuleUniqueIdPState;
import com.rpl.rama.helpers.RamaAssert;
import com.rpl.rama.module.MicrobatchTopology;
import com.rpl.rama.ops.Ops;
import com.rpl.rama.ops.OutputCollector;
import com.rpl.rama.ops.RamaFunction1;
import com.rpl.rama.impl.NativeRamaFunction0;
import com.rpl.rama.impl.Util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.rpl.rama.helpers.TopologyUtils.extractJavaFields;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Grid implements RamaSerializable {
  /** Declare all the pobjects required for the Grid. */

  void declarePStates(final MicrobatchTopology topology) {
  }

  void declareQueries(final Topologies topology) {
  }

  public void declare(final Topologies topologies,
                      final MicrobatchTopology topology) {
    declarePStates(topology);
    declareQueries(topologies);
  }

  public <T> Block handleModifications(
    final String userModTableVar,
    final ModificationConvertorFunction<T> dataConvertor) {

    return Block.create();
  }
}
