package com.rpl.rama.helpers.spatial.loadtest;

import static com.rpl.rama.helpers.TopologyUtils.extractJavaFields;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import com.rpl.rama.AckLevel;
import com.rpl.rama.Agg;
import com.rpl.rama.Block;
import com.rpl.rama.Case;
import com.rpl.rama.Depot;
import com.rpl.rama.Expr;
import com.rpl.rama.LoopVars;
import com.rpl.rama.ModuleInstanceInfo;
import com.rpl.rama.PState;
import com.rpl.rama.Path;
import com.rpl.rama.RamaModule;
import com.rpl.rama.RamaSerializable;
import com.rpl.rama.helpers.ModuleUniqueIdPState;
import com.rpl.rama.helpers.spatial.AddObject;
import com.rpl.rama.helpers.spatial.Grid;
import com.rpl.rama.helpers.spatial.MBR;
import com.rpl.rama.helpers.spatial.RTree;
import com.rpl.rama.helpers.spatial.ModificationCollector;
import com.rpl.rama.helpers.spatial.Vector;
import com.rpl.rama.helpers.statemachine.core.StateMachineState;
import com.rpl.rama.integration.TaskGlobalContext;
import com.rpl.rama.integration.TaskGlobalObject;
import com.rpl.rama.module.MicrobatchTopology;
import com.rpl.rama.ops.Ops;
import com.rpl.rama.ops.RamaFunction3;
import com.rpl.rama.test.InProcessCluster;
import com.rpl.rama.test.LaunchConfig;

import org.geotools.api.data.FeatureSource;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.feature.type.AttributeDescriptor;
import org.geotools.api.geometry.BoundingBox;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import clojure.lang.Counted;
import clojure.lang.PersistentVector;

public class LoadTest {

  public static final Logger LOGGER = LoggerFactory.getLogger(LoadTest.class);

  public static class TigerLoader implements Loader, TaskGlobalObject {
    public Set<CompletableFuture<Map<String, Object>>> pending;
    public ShapefileDataStore dataStore;
    public FeatureIterator<SimpleFeature>  features;

    public TigerLoader() {
      pending = ConcurrentHashMap.newKeySet();
    }

    public int getTotal() {
      return 0;
    }

    @Override
    public void prepareForTask(int taskId, TaskGlobalContext context) {
      File shapeFile = new File(
        new File(".").getAbsolutePath() +
        "/data/tiger/tl_2023_us_uac20/tl_2023_us_uac20.shp");

      try {
        LOGGER.debug("Path: " + shapeFile.toString());
        String baseName = shapeFile.toString().replaceAll("\\.shp$", "");
        File shxFile = new File(baseName + ".shx");
        File dbfFile = new File(baseName + ".dbf");
        System.out.println("Required files:");
        System.out.println("  .shp exists: " + shapeFile.exists());
        System.out.println("  .shx exists: " + shxFile.exists());
        System.out.println("  .dbf exists: " + dbfFile.exists());
        Map<String, Serializable> params = new HashMap<>();
        params.put("url", shapeFile.toURI().toURL());
        params.put("create spatial index", Boolean.FALSE);
        ShapefileDataStoreFactory dataStoreFactory = new ShapefileDataStoreFactory();
        ShapefileDataStore dataStore = (ShapefileDataStore) dataStoreFactory.createNewDataStore(params);
        // dataStore.createSchema(CITY);
        String nameAttribute = "NAME20";
        String typeName = dataStore.getTypeNames()[0];
        LOGGER.debug("typeName = " + typeName);
        LOGGER.debug("typeNames = " + dataStore.getTypeNames().length);
        FeatureSource<SimpleFeatureType, SimpleFeature> featureSource = dataStore.getFeatureSource(typeName);
        SimpleFeatureType schema = featureSource.getSchema();
        for (AttributeDescriptor attr : schema.getAttributeDescriptors()) {
          LOGGER.debug("Available attribute: " + attr.getLocalName());
        }
        FeatureCollection<SimpleFeatureType, SimpleFeature> collection = featureSource.getFeatures(// query
      );
        // Iterate through features and get bounding boxes
        int i = 0;
        features = collection.features();

      } catch (IOException e) {
        LOGGER.error("Error", e);
      }
    }

    @Override
    public void close() throws IOException {
      if (dataStore != null) {
        dataStore.dispose();
      }
    }

    public LoadDataResult loadData(Random random) {
      if (features != null && features.hasNext()) {
        final List<AddObject> ops = new ArrayList<>();
        final String nameAttribute = "NAME20";
        final int numToAppend = 100;
        for (int i = 0; i <= numToAppend; i = i + 1) {
          if (features.hasNext()) {
            SimpleFeature feature = features.next();
            BoundingBox bounds = feature.getBounds();

            MBR mbr = new MBR(new double[]{bounds.getMinX(), bounds.getMinY()},
                              new double[]{bounds.getMaxX(), bounds.getMaxY()});

            ops.add(new AddObject(mbr, feature.getAttribute(nameAttribute)));
            // CompletableFuture<Map<String, Object>> cf =
            //     depot.appendAsync(
            //       new AddObject(mbr, feature.getAttribute(nameAttribute)),
            //       AckLevel.NONE);
            // cf.thenApply((_v) -> loader.pending.remove(cf));
            // loader.pending.add(cf);
          }
        }
        return new LoadDataResult(false, ops);
      } else {
        return new LoadDataResult(true, null);
      }
    }
  }

  public static class LoadData implements RamaSerializable {
    public Boolean allProcessed;
    public Boolean neverProcessed;
    public long processingStart;
    public long processingEnd;
    public long numProcessed;

    public LoadData() {
      allProcessed = false;
      neverProcessed = true;
      processingStart = Instant.now().toEpochMilli();
      processingEnd = 0L;
      numProcessed = 0L;
    }

    public LoadData reset() {
      LOGGER.debug("reset");
      neverProcessed = true;
      allProcessed = false;
      processingStart =  0L;
      processingEnd = 0L;
      numProcessed = 0L;
      return this;
    }

    LoadData someProcessed(long n) {
      LOGGER.debug("someProcessed: n=" + n + ", this=" + this);
      if (neverProcessed) {
        processingStart = Instant.now().toEpochMilli();
        allProcessed = false;
        neverProcessed = false;
      }
      numProcessed = numProcessed + n;
      return this;
    }

    LoadData noneProcessed() {
      LOGGER.debug("noneProcessed: " + this);
      if (!neverProcessed && !allProcessed) {
        processingEnd = Instant.now().toEpochMilli();
        allProcessed = true;
      }
      return this;
    }

    Boolean isAllProcessed() {
      return !neverProcessed && allProcessed;
    }

    Double processingDuration() {
      return Duration.between(
        Instant.ofEpochMilli(processingStart),
        Instant.ofEpochMilli(processingEnd)).toMillis()/1000.0;
    }

    Double processingRate() {
      return numProcessed / processingDuration();
    }

    Long getNumProcessed() {
      return numProcessed;
    }

    @Override
    public String toString() {
      return "LoadData [allProcessed=" + allProcessed +
          ", neverProcessed=" + neverProcessed +
          ", processingStart=" + processingStart +
          ", processingEnd=" + processingEnd +
          ", numProcessed=" + numProcessed + "]";
    }
  }

  public static class QueryData implements RamaSerializable {
    public Boolean neverQueried;
    public long queriesStart;
    public long queriesEnd;
    public long numQueried;

    public QueryData() {
      LOGGER.debug("QueryData");
      neverQueried = true;
      queriesStart = Instant.now().toEpochMilli();
      queriesEnd = 0L;
      numQueried = 0L;
    }

    public QueryData reset() {
      LOGGER.debug("QueryData::reset");
      neverQueried = true;
      queriesStart = 0L;
      queriesEnd = 0L;
      numQueried = 0L;
      return this;
    }

    // QueryData someQueried(long n) {
    //   LOGGER.debug("someQueried: n=" + n + ", this=" + this);
    //   if (neverQueried) {
    //     queriesStart = Instant.now().toEpochMilli();
    //     queriesEnd = Instant.now().toEpochMilli();
    //     neverQueried = false;
    //   } else {
    //     queriesEnd = Instant.now().toEpochMilli();
    //     numQueried = numQueried + n;
    //   }
    //   return this;
    // }

    QueryData someQueried(int n) {
      LOGGER.debug("someQueried: n=" + n + ", this=" + this);
      if (neverQueried) {
        queriesStart = Instant.now().toEpochMilli();
        queriesEnd = Instant.now().toEpochMilli();
        neverQueried = false;
      } else {
        queriesEnd = Instant.now().toEpochMilli();
        numQueried = numQueried + n;
      }
      return this;
    }

    Double queryDuration() {
      return Duration.between(
          Instant.ofEpochMilli(queriesStart),
          Instant.ofEpochMilli(queriesEnd)).toMillis() / 1000.0;
    }

    Double queryRate() {
      return numQueried / queryDuration();
    }

    Long getNumQueried() {
      return numQueried;
    }

    @Override
    public String toString() {
      return "QueryData [neverQueried=" + neverQueried +
          ", queriesStart=" + queriesStart +
          ", queriesEnd=" + queriesEnd +
          ", numQueried=" + numQueried + "]";
    }
  }

  public static class RTreeModule implements RamaModule {
    ModuleUniqueIdPState idGenerator = new ModuleUniqueIdPState("$$objectId");

    @Override
    public void define(Setup setup, Topologies topologies) {
      setup.declareDepot("*depot", Depot.random());
      setup.declareDepot("*statsDepot", Depot.random());
      setup.declareDepot("*queryStatsDepot", Depot.random());

      setup.setLaunchModuleDynamicOption("depot.microbatch.max.records", 20 // 1000
                                         );
      // setup.setLaunchModuleDynamicOption("depot.max.fetch", 1024);

      // setup.setLaunchModuleDynamicOption(
      //   "topology.microbatch.pstate.flush.path.count", 1024);

      MicrobatchTopology m = topologies.microbatch("m");
      m.pstate("$$object", PState.mapSchema(Long.class, Object.class));
      m.pstate("$$loadData", LoadData.class).initialValue(new LoadData());
      m.pstate("$$queryData", QueryData.class).initialValue(new QueryData());

      // This is just a test convenience
      m.pstate("$$objectLookup", PState.mapSchema(Object.class, Long.class));

      idGenerator.declarePState(m);

      // declare the RTree
      final int dimensions = 2;
      final int branchingFactor = 64;
      final int minChildren = 1;
      RTree rTree = new RTree(dimensions,
                              branchingFactor,
                              minChildren,
                              "test");
      rTree.declare(topologies, m);

      // ETL
      m.source("*depot").out("*microbatch")
          .each(Ops.LOG_DEBUG, LOGGER, "Microbatch")
          .batchBlock(Block.keepTrue(false).materialize().out("$$objects"))

          .batchBlock(
            Block
            // .each(Ops.LOG_TRACE, LOGGER, "New Microbatch")
            .explodeMicrobatch("*microbatch").out("*batch")
            .each(Ops.EXPLODE, "*batch").out("*v")
            .macro(idGenerator.genId("*objectId"))
            .macro(extractJavaFields("*v", "*bounds", "*object"))
            // .each(Ops.LOG_TRACE,
            //       LOGGER,
            //       new Expr(Ops.TO_STRING,
            //                "objectId=", "*objectId",
            //                ", MB Process: ", "*v"))
            .hashPartition("$$object", "*objectId")
            .localTransform("$$object",
                            Path.key("*objectId").termVal("*object"))

            .hashPartition("$$objectLookup", "*object")
            .localTransform("$$objectLookup",
                            Path.key("*object").termVal("*objectId"))

            .each(Ops.LOG_TRACE, LOGGER,
                  new Expr(Ops.TO_STRING,
                           "Added object", "*objectId", "*object", "*bounds"))
            .each(Ops.TUPLE, "*bounds", "*objectId").out("*tuple")
            .localTransform("$$objects", Path.afterElem().termVal("*tuple"))
            // TODO remove this hack for number of objects
            .globalPartition()
            .agg(Agg.count()).out("*numObjects")
            .each(Ops.LOG_DEBUG, LOGGER, "numObjects: {}", "*numObjects")

            // .each(Ops.CURRENT_TASK_ID).out("*taskIdTmp")
            .ifTrue(
              new Expr(Ops.IS_POSITIVE, "*numObjects"),
              Block
              .localTransform(
                "$$loadData",
                Path.term(LoadData::someProcessed, "*numObjects")),
              Block
              .localTransform(
                "$$loadData",
                Path.term(LoadData::noneProcessed)))
            // .directPartition("*taskIdTmp")
            // .globalPartition()
            // .depotPartitionAppend("*statsDepot", "*numObjects")
            // .each(Ops.LOG_DEBUG, LOGGER,
            //       new Expr(Ops.TO_STRING, "before handleModifications"))
                      )

            .macro(
              rTree.handleModifications(
                "$$objects",
                (List<Object> data, ModificationCollector collector) -> {
                  collector.addObject(
                    (MBR) data.get(0),
                    (Long) data.get(1));
                }))
          .each(Ops.LOG_DEBUG, LOGGER, "Microbatch done");

      m.source("*statsDepot").out("*microbatch")
          .each(Ops.LOG_DEBUG, LOGGER, "Microbatch statsDepot")
          .explodeMicrobatch("*microbatch").out("*data")
          .ifTrue(
            new Expr(Ops.IS_INSTANCE_OF, Long.class, "*data"),
            Block.ifTrue(
              new Expr(Ops.IS_POSITIVE, "*data"),
              Block
              .localTransform(
                "$$loadData",
                Path.term(LoadData::someProcessed, "*data")),
              Block
              .localTransform(
                "$$loadData",
                Path.term(LoadData::noneProcessed))),
            Block.localTransform("$$loadData", Path.term(LoadData::reset)))
          .each(Ops.LOG_DEBUG, LOGGER, "Microbatch statsDepot done")
          ;

      m.source("*queryStatsDepot").out("*microbatch")
          .each(Ops.LOG_DEBUG, LOGGER, "Microbatch queryStatsDepot")
          .explodeMicrobatch("*microbatch").out("*data")
          .each(Ops.LOG_DEBUG, LOGGER, "Microbatch queryStatsDepot {}", "*data")
          .ifTrue(
            new Expr(Ops.IS_INSTANCE_OF, Integer.class, "*data"),
            Block
            .localTransform(
              "$$queryData",
              Path.term(QueryData::someQueried, "*data")))
          .each(Ops.LOG_DEBUG, LOGGER, "Microbatch statsDepot done")
          ;

      topologies.query("loadData").out("*finalLoadData")
          // .each(Ops.LOG_TRACE, LOGGER,"allProcessed")
          .allPartition()
          .localSelect("$$loadData", Path.stay()).out("*loadData")
          .macro(extractJavaFields("*loadData",
                                   "*numProcessed",
                                   "*processingStart",
                                   "*processingEnd"))
          .each(LoadData::isAllProcessed, "*loadData").out("*allProcessed")
          .keepTrue(new Expr(Ops.GREATER_THAN, "*processingStart", 0L))
          .originPartition()
          .agg(Agg.sum("*numProcessed")).out("*totalProcessed")
          .agg(Agg.min("*processingStart")).out("*minStart")
          .agg(Agg.max("*processingEnd")).out("*maxEnd")
          .agg(Agg.and("*allProcessed")).out("*allAllProcessed")
          .each(Ops.TUPLE,
                "*totalProcessed",
                "*minStart",
                "*maxEnd",
                new Expr(Ops.AND,
                         "*allAllProcessed",
                         new Expr(Ops.IS_POSITIVE, "*totalProcessed")))
                .out("*finalLoadData");

      topologies.query("resetData").out("*out")
          .allPartition()
          .depotPartitionAppend("*statsDepot", "reset")
          .each(Ops.IDENTITY, "reset").out("*reset")
          .originPartition()
          .agg(Agg.last("*reset")).out("*out");

      topologies.query("queryData").out("*finalData")
          // .each(Ops.LOG_TRACE, LOGGER,"allProcessed")
          .allPartition()
          .localSelect("$$queryData", Path.stay()).out("*queryData")
          .macro(extractJavaFields("*queryData",
                                   "*numQueried",
                                   "*queriesStart",
                                   "*queriesEnd"))
          .keepTrue(new Expr(Ops.GREATER_THAN, "*queriesStart", 0L))
          .originPartition()
          .agg(Agg.sum("*numQueried")).out("*totalQueried")
          .agg(Agg.min("*queriesStart")).out("*minStart")
          .agg(Agg.max("*queriesEnd")).out("*maxEnd")
          .each(Ops.TUPLE,
                "*totalQueried",
                "*minStart",
                "*maxEnd")
          .out("*finalData");


    }
  }

  public static String RTreeModuleName = RTreeModule.class.getName();

  public static class GridModule implements RamaModule {
    ModuleUniqueIdPState idGenerator = new ModuleUniqueIdPState("$$objectId");

    @Override
    public void define(Setup setup, Topologies topologies) {
      setup.declareDepot("*depot", Depot.random());
      setup.declareDepot("*statsDepot", Depot.random());

      setup.setLaunchModuleDynamicOption("depot.microbatch.max.records", 20 // 1000
                                         );
      // setup.setLaunchModuleDynamicOption("depot.max.fetch", 1024);

      // setup.setLaunchModuleDynamicOption(
      //   "topology.microbatch.pstate.flush.path.count", 1024);

      MicrobatchTopology m = topologies.microbatch("m");
      m.pstate("$$object", PState.mapSchema(Long.class, Object.class));
      m.pstate("$$loadData", LoadData.class).initialValue(new LoadData());

      // This is just a test convenience
      m.pstate("$$objectLookup", PState.mapSchema(Object.class, Long.class));

      idGenerator.declarePState(m);

      // declare the Grid
      final int[] extents = {10, 10};
      final MBR bounds = new MBR(new double[] { 0, 0 },
                                 new double[] { 100, 1000});
      Grid grid = new Grid(bounds, extents, "test");
      grid.declare(topologies, m);

      // ETL
      m.source("*depot").out("*microbatch")
          .each(Ops.LOG_DEBUG, LOGGER, "Microbatch")
          .batchBlock(Block.keepTrue(false).materialize().out("$$objects"))

          .batchBlock(
            Block
            // .each(Ops.LOG_TRACE, LOGGER, "New Microbatch")
            .explodeMicrobatch("*microbatch").out("*batch")
            .each(Ops.EXPLODE, "*batch").out("*v")
            .macro(idGenerator.genId("*objectId"))
            .macro(extractJavaFields("*v", "*bounds", "*object"))
            // .each(Ops.LOG_TRACE,
            //       LOGGER,
            //       new Expr(Ops.TO_STRING,
            //                "objectId=", "*objectId",
            //                ", MB Process: ", "*v"))
            .hashPartition("$$object", "*objectId")
            .localTransform("$$object",
                            Path.key("*objectId").termVal("*object"))

            .hashPartition("$$objectLookup", "*object")
            .localTransform("$$objectLookup",
                            Path.key("*object").termVal("*objectId"))

            .each(Ops.LOG_TRACE, LOGGER,
                  new Expr(Ops.TO_STRING,
                           "Added object", "*objectId", "*object", "*bounds"))
            .each(Ops.TUPLE, "*bounds", "*objectId").out("*tuple")
            .localTransform("$$objects", Path.afterElem().termVal("*tuple"))
            // TODO remove this hack for number of objects
            .globalPartition()
            .agg(Agg.count()).out("*numObjects")
            .each(Ops.LOG_DEBUG, LOGGER, "numObjects: {}", "*numObjects")

            // .each(Ops.CURRENT_TASK_ID).out("*taskIdTmp")
            .ifTrue(
              new Expr(Ops.IS_POSITIVE, "*numObjects"),
              Block
              .localTransform(
                "$$loadData",
                Path.term(LoadData::someProcessed, "*numObjects")),
              Block
              .localTransform(
                "$$loadData",
                Path.term(LoadData::noneProcessed)))
            // .directPartition("*taskIdTmp")
            // .globalPartition()
            // .depotPartitionAppend("*statsDepot", "*numObjects")
            // .each(Ops.LOG_DEBUG, LOGGER,
            //       new Expr(Ops.TO_STRING, "before handleModifications"))
                      )

            .macro(
              grid.handleModifications(
                "$$objects",
                (List<Object> data, ModificationCollector collector) -> {
                  collector.addObject(
                    (MBR) data.get(0),
                    (Long) data.get(1));
                }))
          .each(Ops.LOG_DEBUG, LOGGER, "Microbatch done");

      m.source("*statsDepot").out("*microbatch")
          .each(Ops.LOG_DEBUG, LOGGER, "Microbatch statsDepot")
          .explodeMicrobatch("*microbatch").out("*data")
          .ifTrue(
            new Expr(Ops.IS_INSTANCE_OF, Long.class, "*data"),
            Block.ifTrue(
              new Expr(Ops.IS_POSITIVE, "*data"),
              Block
              .localTransform(
                "$$loadData",
                Path.term(LoadData::someProcessed, "*data")),
              Block
              .localTransform(
                "$$loadData",
                Path.term(LoadData::noneProcessed))),
            Block.localTransform("$$loadData", Path.term(LoadData::reset)))
          .each(Ops.LOG_DEBUG, LOGGER, "Microbatch statsDepot done")
          ;

      topologies.query("loadData").out("*finalLoadData")
          // .each(Ops.LOG_TRACE, LOGGER,"allProcessed")
          .localSelect("$$loadData", Path.stay()).out("*loadData")
          .macro(extractJavaFields("*loadData",
                                   "*numProcessed",
                                   "*processingStart",
                                   "*processingEnd"))
          .each(LoadData::isAllProcessed, "*loadData").out("*allProcessed")
          .keepTrue(new Expr(Ops.GREATER_THAN, "*processingStart", 0L))
          .originPartition()
          .agg(Agg.sum("*numProcessed")).out("*totalProcessed")
          .agg(Agg.min("*processingStart")).out("*minStart")
          .agg(Agg.max("*processingEnd")).out("*maxEnd")
          .agg(Agg.and("*allProcessed")).out("*allAllProcessed")
          .each(Ops.TUPLE,
                "*totalProcessed",
                "*minStart",
                "*maxEnd",
                new Expr(Ops.AND,
                         "*allAllProcessed",
                         new Expr(Ops.IS_POSITIVE, "*totalProcessed")))
                .out("*finalLoadData");

      topologies.query("resetData").out("*out")
          .allPartition()
          .depotPartitionAppend("*statsDepot", "reset")
          .each(Ops.IDENTITY, "reset").out("*reset")
          .originPartition()
          .agg(Agg.last("*reset")).out("*out");
    }
  }

  public static String GridModuleName = GridModule.class.getName();

  public static class LoadModule implements RamaModule {

    static long seed = 0; // (new Random()).nextLong();
    static boolean enabled = false;

    String spatialModuleName;

    LoadTestStateMachine statemachine = new LoadTestStateMachine();

    static volatile RamaFunction3<String,String,Boolean, Boolean> pauseFn =
        null;

    LoadModule(String spatialModuleName) {
      this.spatialModuleName = spatialModuleName;
    }

    LoadModule() {
      this.spatialModuleName = GridModuleName;
    }

    static Boolean setTopologyActive(String moduleName,
                                     String topologyName,
                                     Boolean activeFlag) {
      if (pauseFn == null) {
        RamaClient.setTopologyActive(moduleName, topologyName, activeFlag);
      } else {
        pauseFn.invoke(moduleName, topologyName, activeFlag);
      }
      return true;
    }

    public static Random mkRandom() {
      LOGGER.debug("Make random with seed: " + seed);
      Random random = new Random(seed);
      seed = random.nextLong();
      return random;
    }

    public static boolean setEnabled(Boolean flag) {
      enabled = flag;
      return enabled;
    }

    public static boolean isNotEnabled() {
      return !enabled;
    }

    static PersistentVector nextBatch(Iterator<List<LoadDataResult>> iter)
    {
      PersistentVector batch = Vector.empty();
      for (int i = 0; i < 128; i=i+1) {
        if (iter.hasNext()) {
          batch = Vector.conj(batch, iter.next());
        } else {
          break;
        }
      }
      return batch;
    }

    @Override
    public void define(Setup setup, Topologies topologies) {
      statemachine.stateMachine.define(
        setup,
        topologies,
        LoadTestStateMachine.LoadTestState.DISABLE_MB);

      final MBR bounds = new MBR(new double[] { 0.0, 0.0 },
                                 new double[] { 120.0, 120.0 });

      seed = (new Random()).nextLong();

        // N 2node cluster 2000 gives 85% load
      int numNodes = 1;
      setup.declareObject("*loader",
                          new RandomObjectGenerator(bounds, // 40 * 2048
                                                    1 * 100));
      setup.declareObject("*querent",
                          new RandomQueryGenerator(bounds, // 40
                                                   1 * 2048));
      setup.clusterDepot("*depot2", spatialModuleName, "*depot");
      setup.clusterDepot("*queryStatsDepot", spatialModuleName, "*queryStatsDepot");
      setup.clusterQuery("*loadDataQuery", spatialModuleName, "loadData");
      setup.clusterQuery("*resetDataQuery", spatialModuleName, "resetData");
      setup.clusterQuery("*queryDataQuery", spatialModuleName, "queryData");
      setup.clusterQuery("*objectsInBounds", spatialModuleName, "objectsInBounds");

      MicrobatchTopology m = topologies.microbatch("m");

      final String smStateVar = "*smState";

      m.source("*smDepot").out("*microbatch")
          .batchBlock(
            Block
            .explodeMicrobatch("*microbatch").out("*mbValue")
            .each(Ops.CURRENT_TASK_ID).out("*localTaskId")
            .directPartition(0)
            .localSelect("$$sm", Path.stay()).out(smStateVar)
            .each(StateMachineState<LoadTestStateMachine.LoadTestState>::getCurrentState,
                  smStateVar).out("*state")
            .directPartition("*localTaskId")
            .each(Ops.LOG_DEBUG, LOGGER,
                  new Expr(Ops.TO_STRING, "task state: ", "*state"))
            .cond(
              Case.create(
                new Expr(Ops.EQUAL,
                         "*state",
                         LoadTestStateMachine.LoadTestState.DISABLE_MB))
              .each(LoadModule::setTopologyActive, spatialModuleName, "m", false)
              .each(Ops.LOG_DEBUG, LOGGER, "Disabled topology")
              .each(Ops.IDENTITY,
                    LoadTestStateMachine.LoadTestState.LOAD_DATA)
              .out("*nextState")
              .macro(statemachine.stateMachine.transitionTo("*nextState")),

              Case.create(
                new Expr(Ops.EQUAL,
                         "*state",
                         LoadTestStateMachine.LoadTestState.LOAD_DATA))
              .allPartition()
              .each(Ops.LOG_DEBUG, LOGGER, "LOAD DATA")
              .each(LoadModule::mkRandom).out("*random")
              .each(Loader::loadData, "*loader", "*random").out("*result")
              .macro(extractJavaFields("*result", "*addObjects", "*done"))
              .ifTrue(
                "*done",
                Block
                .each(Ops.CURRENT_TASK_ID).out("*taskId")
                .each(Ops.IDENTITY,
                      LoadTestStateMachine.LoadTestSignal.LOAD_COMPLETE)
                .out("*signal")
                .macro(statemachine.stateMachine.setSignal("*taskId", "*signal")),
                Block
                .loopWithVars(
                  LoopVars
                  .var("*iter", new Expr(List<LoadDataResult>::iterator, "*addObjects"))
                  // .var("*ackLevel", new Expr(Ops.IDENTITY, AckLevel.ACK))
                  ,
                  Block
                  .ifTrue(
                    new Expr(Iterator<List<LoadDataResult>>::hasNext, "*iter"),
                    Block
                    .each(LoadModule::nextBatch, "*iter").out("*batch")
                    // need to has to use the mirrored partition
                    .hashPartition("*depot2", "*batch")
                    .depotPartitionAppend("*depot2", "*batch", AckLevel.NONE)
                    // .ifTrue(
                    //   new Expr(Ops.EQUAL, "*ackLevel", AckLevel.ACK),
                    //   Block.depotPartitionAppend("*depot2", "*batch", AckLevel.ACK),
                    //   Block.depotPartitionAppend("*depot2", "*batch", AckLevel.NONE))
                    .continueLoop("*iter"// ,
                                  // new Expr(Ops.IDENTITY, AckLevel.NONE)
                                  ))))
              .each(Ops.LOG_DEBUG, LOGGER, "LOAD DATA DONE"),

              Case.create(
                new Expr(Ops.EQUAL,
                         "*state",
                         LoadTestStateMachine.LoadTestState.ENABLE_MB))
              .each(Ops.LOG_DEBUG, LOGGER, "ENABLE_MB")
              .ifTrue(new Expr(LoadModule::isNotEnabled),
                Block
                .each(Ops.LOG_DEBUG, LOGGER, "ENABLE_MB enabling")
                .each(LoadModule::setTopologyActive, spatialModuleName, "m", true)
                .each(LoadModule::setEnabled, true),
                Block
                .each(Ops.LOG_DEBUG, LOGGER, "ENABLE_MB already enabled"))
              .each(Loader::getTotal, "*loader").out("*totalAppends")
              .each(Ops.LOG_DEBUG, LOGGER, "Total appends: {}", "*totalAppends")

              // .globalPartition()
              // .localTransform("$$loadData", Path.term(LoadData::reset))
              // .each(Ops.IDENTITY,
              //         LoadTestStateMachine.LoadTestState.TIME_PROCESSING)
              // .out("*nextState")
              // .macro(statemachine.stateMachine.transitionTo("*nextState"))
              ,

              Case.create(
                new Expr(
                  Ops.EQUAL,
                  "*state",
                  LoadTestStateMachine.LoadTestState.INITIAL_PROCESSING))
              .each(Ops.LOG_DEBUG, LOGGER, "INITIAL_PROCESSING"),

              Case.create(
                new Expr(Ops.EQUAL,
                         "*state",
                         LoadTestStateMachine.LoadTestState.RESET_STATS))
              .each(Ops.LOG_DEBUG, LOGGER, "RESET_STATES")
              .invokeQuery("*resetDataQuery").out("*xxx")
              .each(Ops.IDENTITY,
                    LoadTestStateMachine.LoadTestState.TIME_PROCESSING)
              .out("*nextState")
              .macro(statemachine.stateMachine.transitionTo("*nextState")),

              Case.create(
                new Expr(Ops.EQUAL,
                         "*state",
                         LoadTestStateMachine.LoadTestState.TIME_PROCESSING))
              .each(Ops.LOG_DEBUG, LOGGER, "TIME_PROCESSING")
              .invokeQuery("*loadDataQuery").out("*loadData")
              .each(Ops.LOG_DEBUG, LOGGER,
                    new Expr(Ops.TO_STRING, "loadData: ", "*loadData"))
              .each(Ops.EXPAND, "*loadData").out("*totalProcessed",
                                                 "*minStart",
                                                 "*maxEnd",
                                                 "*allProcessed")
              .each(Ops.LOG_DEBUG, LOGGER, "allProcessed: {}", "*allProcessed")
              .ifTrue(
                "*allProcessed",
                Block
                .each(Ops.IDENTITY,
                      LoadTestStateMachine.LoadTestState.QUERY_PERFORMANCE)
                .out("*nextState")
                .macro(statemachine.stateMachine.transitionTo("*nextState"))
                .each(Ops.MINUS_LONG, "*maxEnd", "*minStart").out("*duration")
                .each(Ops.DIV, "*totalProcessed", "*duration").out("*ratePerMs")
                .each(Ops.TIMES_LONG, "*ratePerMs", 1000.0).out("*rate")
                .each(Ops.LOG_INFO, LOGGER,
                      new Expr(Ops.TO_STRING,
                               "Processed: ", "*totalProcessed",
                               " records in ", "*duration",
                               "secs, processing rate (records/sec): ",
                               "*rate"))),

              Case.create(
                new Expr(Ops.EQUAL,
                         "*state",
                         LoadTestStateMachine.LoadTestState.QUERY_PERFORMANCE))
              .allPartition()
              .each(Ops.LOG_DEBUG, LOGGER, "QUERY_PERFORMANCE")
              .each(LoadModule::mkRandom).out("*random")
              .each(Querent::generateQuery, "*querent", "*random").out("*result")
              .macro(extractJavaFields("*result", "*bounds", "*done"))
              .ifTrue(
                "*done",
                Block
                .each(Ops.CURRENT_TASK_ID).out("*taskId")
                .each(Ops.IDENTITY,
                      LoadTestStateMachine.LoadTestSignal.QUERY_COMPLETE)
                .out("*signal")
                .macro(statemachine.stateMachine.setSignal("*taskId", "*signal")),
                // .hashPartition("*depot2", "*batch")
                Block
                .invokeQuery("*objectsInBounds", "*bounds").out("*objects")
                .each(Ops.LOG_ERROR, LOGGER, "Received objects")
                .hashPartition("*queryStatsDepot", 1) // TODO make this random
                .depotPartitionAppend("*queryStatsDepot", 1, AckLevel.APPEND_ACK))
              .each(Ops.LOG_DEBUG, LOGGER, "QUERY_PERFORMANCE DONE"),

              Case.create(
                new Expr(Ops.EQUAL,
                         "*state",
                         LoadTestStateMachine.LoadTestState.QUERY_DONE))
              .each(Ops.IDENTITY,
                    LoadTestStateMachine.LoadTestState.DONE)
              .out("*nextState")
              .macro(statemachine.stateMachine.transitionTo("*nextState"))

              .invokeQuery("*queryDataQuery").out("*queryData")
              .each(Ops.LOG_DEBUG, LOGGER,
                    new Expr(Ops.TO_STRING, "queryData: ", "*queryData"))
              .each(Ops.EXPAND, "*queryData").out("*totalQueries",
                                                  "*minStart",
                                                  "*maxEnd")

              .each(Ops.MINUS_LONG, "*maxEnd", "*minStart").out("*duration")
              .each(Ops.DIV, "*totalQueries", "*duration").out("*qratePerMs")
              .each(Ops.TIMES_LONG, "*qratePerMs", 1000.0).out("*qrate")
              .each(Ops.LOG_INFO, LOGGER,
                    new Expr(Ops.TO_STRING,
                             "# Queries: ", "*totalQueries",
                             " in ", "*duration",
                             "secs, query rate (queries/sec): ",
                             "*qrate")),

              Case.create(
                new Expr(Ops.EQUAL,
                         "*state",
                         LoadTestStateMachine.LoadTestState.DONE))));
    }
  }

  @Test
  public void rtreeLoadTestTest() throws Exception
  {
    LOGGER.error("loadTestTest");
    try (InProcessCluster cluster = InProcessCluster.create()) {
      LOGGER.error("Launching spatial index module");
      final RamaModule RTreeModule = new RTreeModule();
      cluster.launchModule(RTreeModule, new LaunchConfig(2, 2));


      LOGGER.error("Launching perf test module");
      LoadModule.pauseFn =
        (String moduleName, String topologyName, Boolean activeFlag) -> {
        if (activeFlag) {
          LOGGER.error("Enable topology, moduleName: "+moduleName
                       + ", topologyName: "+topologyName);
          cluster.resumeMicrobatchTopology(moduleName, topologyName);
        } else {
          LOGGER.error("Disable topology, moduleName: "+moduleName
                       + ", topologyName: "+topologyName);
          cluster.pauseMicrobatchTopology(moduleName, topologyName);
        }
        return activeFlag;
      };

      final RamaModule module = new LoadModule(RTreeModuleName);
      cluster.launchModule(module, new LaunchConfig(4, 1));
      LOGGER.error("Launched perf test module");
      final PState smState =
        cluster.clusterPState(LoadModule.class.getName(), "$$sm");
      Thread.sleep(30000);

      LOGGER.error("Start waiting for test completion");
      StateMachineState<LoadTestStateMachine.LoadTestState> state = null;
      for (int i=0; i<1000; i=i+1) {
         state = smState.selectOne(Path.stay());
         if (state.currentState == LoadTestStateMachine.LoadTestState.DONE) {
           break;
         }
         LOGGER.error("Waiting for state machine to complete");
         Thread.sleep(10000);
      }
      assertEquals(LoadTestStateMachine.LoadTestState.DONE, state.currentState);
    }
    LOGGER.error("loadTestTest done");
  }

  @Test
  public void gridLoadTestTest() throws Exception
  {
    LOGGER.error("loadTestTest");
    try (InProcessCluster cluster = InProcessCluster.create()) {
      LOGGER.error("Launching spatial index module");
      final RamaModule GridModule = new GridModule();
      cluster.launchModule(GridModule, new LaunchConfig(2, 2));


      LOGGER.error("Launching perf test module");
      LoadModule.pauseFn =
        (String moduleName, String topologyName, Boolean activeFlag) -> {
        if (activeFlag) {
          LOGGER.error("Enable topology, moduleName: "+moduleName
                       + ", topologyName: "+topologyName);
          cluster.resumeMicrobatchTopology(moduleName, topologyName);
        } else {
          LOGGER.error("Disable topology, moduleName: "+moduleName
                       + ", topologyName: "+topologyName);
          cluster.pauseMicrobatchTopology(moduleName, topologyName);
        }
        return activeFlag;
      };

      final RamaModule module = new LoadModule(GridModuleName);
      cluster.launchModule(module, new LaunchConfig(4, 1));
      LOGGER.error("Launched perf test module");
      final PState smState =
        cluster.clusterPState(LoadModule.class.getName(), "$$sm");
      Thread.sleep(30000);

      LOGGER.error("Start waiting for test completion");
      StateMachineState<LoadTestStateMachine.LoadTestState> state = null;
      for (int i=0; i<1000; i=i+1) {
         state = smState.selectOne(Path.stay());
         if (state.currentState == LoadTestStateMachine.LoadTestState.DONE) {
           break;
         }
         LOGGER.error("Waiting for state machine to complete");
         Thread.sleep(10000);
      }
      assertEquals(LoadTestStateMachine.LoadTestState.DONE, state.currentState);
    }
    LOGGER.error("loadTestTest done");
  }
}
