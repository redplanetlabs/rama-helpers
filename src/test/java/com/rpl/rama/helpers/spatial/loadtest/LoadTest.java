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
import com.rpl.rama.helpers.spatial.MBR;
import com.rpl.rama.helpers.spatial.RTree;
import com.rpl.rama.helpers.spatial.RTreeCollector;
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

public class LoadTest {

  public static final Logger LOGGER = LoggerFactory.getLogger(LoadTest.class);

  public static class TigerLoader implements Loader, TaskGlobalObject {
    public Set<CompletableFuture<Map<String, Object>>> pending;
    public ShapefileDataStore dataStore;
    public FeatureIterator<SimpleFeature>  features;

    public TigerLoader() {
      pending = ConcurrentHashMap.newKeySet();
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

  // static LoadDataResult loadTigerData(final Loader loader) {
  //   if (loader.features != null && loader.features.hasNext()) {
  //     final List<AddObject> ops = new ArrayList<>();
  //     final String nameAttribute = "NAME20";
  //     final int numToAppend = 100;
  //     for (int i = 0; i <= numToAppend; i = i + 1) {
  //         if (loader.features.hasNext()) {
  //           SimpleFeature feature = loader.features.next();
  //           BoundingBox bounds = feature.getBounds();

  //           MBR mbr = new MBR(new double[]{bounds.getMinX(), bounds.getMinY()},
  //                             new double[]{bounds.getMaxX(), bounds.getMaxY()});

  //           ops.add(new AddObject(mbr, feature.getAttribute(nameAttribute)));
  //           // CompletableFuture<Map<String, Object>> cf =
  //           //     depot.appendAsync(
  //           //       new AddObject(mbr, feature.getAttribute(nameAttribute)),
  //           //       AckLevel.NONE);
  //           // cf.thenApply((_v) -> loader.pending.remove(cf));
  //           // loader.pending.add(cf);
  //         }
  //       }
  //     return new LoadDataResult(false, ops);
  //   } else {
  //     return new LoadDataResult(true, null);
  //   }
  // }

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

    LoadData someProcessed(int n) {
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

  public static class SpatialModule implements RamaModule {
    ModuleUniqueIdPState idGenerator = new ModuleUniqueIdPState("$$objectId");

    @Override
    public void define(Setup setup, Topologies topologies) {
      setup.declareDepot("*depot", Depot.random());
      setup.declareDepot("*statsDepot", Depot.random());

      setup.setLaunchModuleDynamicOption("depot.microbatch.max.records", 100);

      MicrobatchTopology m = topologies.microbatch("m");
      m.pstate("$$object", PState.mapSchema(Long.class, Object.class));
      m.pstate("$$loadData", LoadData.class)
          .global()
          .initialValue(new LoadData());

      // This is just a test convenience
      m.pstate("$$objectLookup", PState.mapSchema(Object.class, Long.class));

      idGenerator.declarePState(m);

      // declare the RTree
      final int dimensions = 2;
      final int branchingFactor = 8;
      final int minChildren = 1;
      RTree rTree = new RTree(dimensions,
                              branchingFactor,
                              minChildren,
                              "test");
      rTree.declare(topologies, m);

      // ETL
      m.source("*depot").out("*microbatch")
          .batchBlock(
            Block
            // .each(Ops.LOG_TRACE, LOGGER, "New Microbatch")
            .explodeMicrobatch("*microbatch").out("*v")
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
            // .each(Ops.LOG_TRACE, LOGGER,
            //       new Expr(Ops.TO_STRING,
            //                "Added object", "*objectId", "*object", "*bounds"))
            .globalPartition()
            .agg(Agg.list(new Expr(Ops.TUPLE,
                                   "*bounds",
                                   "*objectId")))
            .out("$$objects")
            .localSelect("$$objects",
                         Path.stay().view(Counted::count)).out("*numObjects")
            .each(Ops.LOG_DEBUG, LOGGER,
                  new Expr(Ops.TO_STRING, "numObjects: ", "*numObjects"))
            .ifTrue(
              new Expr(Ops.IS_POSITIVE, "*numObjects"),
              Block.localTransform(
                "$$loadData",
                Path.term(LoadData::someProcessed, "*numObjects")),
              Block.localTransform(
                "$$loadData",
                Path.term(LoadData::noneProcessed)))
            .macro(
              rTree.handleModifications(
                "$$objects",
                (List<Object> data, RTreeCollector collector) -> {
                  collector.addObject(
                    (MBR) data.get(0),
                    (Long) data.get(1));
                })));

      m.source("*statsDepot").out("*microbatch")
          .globalPartition()
          .localTransform("$$loadData", Path.term(LoadData::reset));

      topologies.query("loadData").out("*finalLoadData")
          // .each(Ops.LOG_TRACE, LOGGER,"allProcessed")
          .globalPartition()
          .localSelect("$$loadData", Path.stay()).out("*loadData")
          .originPartition()
          .agg(Agg.last("*loadData")).out("*finalLoadData");

      topologies.query("resetData").out("*out")
          .globalPartition()
          .depotPartitionAppend("*statsDepot", "reset")
          .each(Ops.IDENTITY, "reset").out("*reset")
          .originPartition()
          .agg(Agg.last("*reset")).out("*out");
    }
  }

  public static String spatialModuleName = SpatialModule.class.getName();

  public static class Module implements RamaModule {

    static long seed = 0;

    LoadTestStateMachine statemachine = new LoadTestStateMachine();

    static volatile RamaFunction3<String,String,Boolean, Boolean> pauseFn = null;

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
      Random random = new Random(seed);
      seed = random.nextLong();
      return random;
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

      setup.declareObject("*loader", new RandomObjectGenerator(bounds));
      setup.clusterDepot("*depot2", SpatialModule.class.getName(), "*depot");
      setup.clusterQuery("*loadDataQuery", SpatialModule.class.getName(), "loadData");
      setup.clusterQuery("*resetDataQuery", SpatialModule.class.getName(), "resetData");

      MicrobatchTopology m = topologies.microbatch("m");

      // m.pstate("$$inProgressAppends", PState.mapSchema(Long.class, Long.class));

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
              .each(Module::setTopologyActive, spatialModuleName, "m", false)
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
              .each(Module::mkRandom).out("*random")
              .each(Loader::loadData, "*loader", "*random").out("*result")
              .macro(extractJavaFields("*result", "*addObjects", "*done"))
              .ifTrue(
                "*done",
                Block
                .each(Ops.CURRENT_TASK_ID).out("*taskId")
                .each(Ops.IDENTITY, LoadTestStateMachine.LoadTestSignal.LOAD_COMPLETE).out("*signal")
                .macro(statemachine.stateMachine.setSignal("*taskId", "*signal")),
                Block
                .loopWithVars(
                  LoopVars.var("*iter", new Expr(List<LoadDataResult>::iterator, "*addObjects")),
                  Block
                  .ifTrue(
                    new Expr(Iterator<List<LoadDataResult>>::hasNext, "*iter"),
                    Block
                    .each(Iterator<List<LoadDataResult>>::next, "*iter").out("*addObject")
                    .hashPartition("*depot2", "*addObject")
                    .depotPartitionAppend("*depot2", "*addObject")
                    .continueLoop("*iter"))))
              .each(Ops.LOG_DEBUG, LOGGER, "LOAD DATA DONE"),

              Case.create(
                new Expr(Ops.EQUAL,
                         "*state",
                         LoadTestStateMachine.LoadTestState.ENABLE_MB))
              .each(Ops.LOG_DEBUG, LOGGER, "TIME_PROCESSING")
              .each(Module::setTopologyActive, spatialModuleName, "m", true)
              .invokeQuery("*resetDataQuery").out("*xxx")
              // .globalPartition()
              // .localTransform("$$loadData", Path.term(LoadData::reset))
              // .each(Ops.IDENTITY,
              //         LoadTestStateMachine.LoadTestState.TIME_PROCESSING)
              // .out("*nextState")
              // .macro(statemachine.stateMachine.transitionTo("*nextState"))
              ,

              Case.create(
                new Expr(Ops.EQUAL,
                         "*state",
                         LoadTestStateMachine.LoadTestState.TIME_PROCESSING))
              .each(Ops.LOG_DEBUG, LOGGER, "TIME_PROCESSING")
              .invokeQuery("*loadDataQuery").out("*loadData")
              .each(Ops.LOG_DEBUG, LOGGER,
                    new Expr(Ops.TO_STRING, "loadData: ", "*loadData"))
              .ifTrue(
                new Expr(LoadData::isAllProcessed, "*loadData"),
                Block
                .each(Ops.IDENTITY,
                      LoadTestStateMachine.LoadTestState.QUERY_PERFORMANCE)
                .out("*nextState")
                .macro(statemachine.stateMachine.transitionTo("*nextState"))
                .each(Ops.LOG_INFO, LOGGER,
                      new Expr(Ops.TO_STRING,
                               "Processed: ",
                               new Expr(LoadData::getNumProcessed, "*loadData"),
                               " records in ",
                               new Expr(LoadData::processingDuration, "*loadData"),
                               "secs, processing rate (records/sec): ",
                               new Expr(LoadData::processingRate, "*loadData")))),

              Case.create(
                new Expr(Ops.EQUAL,
                         "*state",
                         LoadTestStateMachine.LoadTestState.QUERY_PERFORMANCE))
              .each(Ops.LOG_DEBUG, LOGGER, "QUERY_PERFORMANCE")
              .each(Ops.IDENTITY, LoadTestStateMachine.LoadTestState.DONE)
              .out("*nextState")
              .macro(statemachine.stateMachine.transitionTo("*nextState")),

              Case.create(
                new Expr(Ops.EQUAL,
                         "*state",
                         LoadTestStateMachine.LoadTestState.DONE))));
    }
  }

  @Test
  public void loadTestTest() throws Exception
  {
    LOGGER.error("loadTestTest");
    try (InProcessCluster cluster = InProcessCluster.create()) {
      LOGGER.error("Launching spatial index module");
      final RamaModule spatialModule = new SpatialModule();
      cluster.launchModule(spatialModule, new LaunchConfig(2, 2));


      LOGGER.error("Launching perf test module");
      Module.pauseFn =
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

      final RamaModule module = new Module();
      cluster.launchModule(module, new LaunchConfig(2, 2));
      LOGGER.error("Launched perf test module");
      final PState smState =
        cluster.clusterPState(Module.class.getName(), "$$sm");
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
