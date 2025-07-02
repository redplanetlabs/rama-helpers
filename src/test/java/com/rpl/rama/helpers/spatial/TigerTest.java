package com.rpl.rama.helpers.spatial;


import com.rpl.rama.AckLevel;
import com.rpl.rama.Agg;
import com.rpl.rama.Block;
import com.rpl.rama.Depot;
import com.rpl.rama.Expr;
import com.rpl.rama.Path;
import com.rpl.rama.PState;
import com.rpl.rama.QueryTopologyClient;
import com.rpl.rama.RamaModule;
import com.rpl.rama.helpers.ModuleUniqueIdPState;
import static com.rpl.rama.helpers.TopologyUtils.extractJavaFields;

import com.rpl.rama.module.MicrobatchTopology;
import com.rpl.rama.ops.Ops;
import com.rpl.rama.test.InProcessCluster;
import com.rpl.rama.test.LaunchConfig;


import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.geotools.api.data.FeatureSource;
import org.geotools.api.data.Query;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.feature.type.AttributeDescriptor;
import org.geotools.api.filter.Filter;
import org.geotools.api.geometry.BoundingBox;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.filter.text.cql2.CQL;

import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TigerTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(TigerTest.class);

  /** A module implementing a 2D spacial index of String values */
  public static class Module implements RamaModule {
    ModuleUniqueIdPState idGenerator = new ModuleUniqueIdPState("$$objectId");

    @Override
    public void define(Setup setup, Topologies topologies) {
      setup.declareDepot("*depot", Depot.random());

      MicrobatchTopology m = topologies.microbatch("m");
      m.pstate("$$object", PState.mapSchema(Long.class, Object.class));

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
            .each(Ops.LOG_ERROR, LOGGER, "New Microbatch")
            .explodeMicrobatch("*microbatch").out("*v")
            .macro(idGenerator.genId("*objectId"))
            .macro(extractJavaFields("*v", "*bounds", "*object"))
            .each(Ops.LOG_ERROR,
                  LOGGER,
                  new Expr(Ops.TO_STRING,
                           "objectId=", "*objectId",
                           ", MB Process: ", "*v"))
            .hashPartition("$$object", "*objectId")
            .localTransform("$$object",
                            Path.key("*objectId").termVal("*object"))

            .hashPartition("$$objectLookup", "*object")
            .localTransform("$$objectLookup",
                            Path.key("*object").termVal("*objectId"))
            .each(Ops.PRINTLN,
                  "Added object",
                  "*objectId",
                  "*object",
                  "*bounds")
            .globalPartition()
            .agg(Agg.list(new Expr(Ops.TUPLE,
                                   "*bounds",
                                   "*objectId"))).out("$$objects"))
            .macro(
              rTree.handleModifications(
                "$$objects",
                (List<Object> data, ModificationCollector collector) -> {
                  collector.addObject(
                    (MBR)data.get(0),
                    (Long)data.get(1));}));
    }
  }

  @Test
  public void tigerTest() throws Exception {

    try(InProcessCluster cluster = InProcessCluster.create()) {
      final RamaModule module = new Module();
      cluster.launchModule(module, new LaunchConfig(4, 3));

      final Depot depot = cluster.clusterDepot(Module.class.getName(), "*depot");
      final PState nodes = cluster.clusterPState(Module.class.getName(), "$$test__nodes");
      final PState root = cluster.clusterPState(Module.class.getName(), "$$test__root");
      final PState object = cluster.clusterPState(Module.class.getName(), "$$object");
      final PState objectLookup = cluster.clusterPState(Module.class.getName(), "$$objectLookup");
      final QueryTopologyClient q = cluster.clusterQuery(Module.class.getName(), "objectsInBounds");
      final QueryTopologyClient<Boolean> verify
          = cluster.clusterQuery(Module.class.getName(), "verifyTree");
      final QueryTopologyClient<List<String>> dumpDot
          = cluster.clusterQuery(Module.class.getName(), "dumpDot");
      final QueryTopologyClient<List<Long>> dump
          = cluster.clusterQuery(Module.class.getName(), "dumpTree");
      final QueryTopologyClient<List<List<Object>>> dumpBounds
          = cluster.clusterQuery(Module.class.getName(), "dumpBounds");

      final QueryTopologyClient<List<List<Object>>> boundsStats
          = cluster.clusterQuery(Module.class.getName(), "boundsStats");

      LOGGER.debug("START");

      // Shapefiles can be download from
      // https://www.census.gov/cgi-bin/geo/shapefiles/index.php
      //
      // Some datasets are Urban Areas (uac20), Places, Counties, and county
      // sub-divisions.
      File shapeFile = new File(
        new File(".").getAbsolutePath() +
        "/data/tiger/tl_2023_us_uac20/tl_2023_us_uac20.shp");

      int n = loadTigerFile(depot, shapeFile);

      cluster.waitForMicrobatchProcessedCount(
        module.getClass().getName(), "m", n);


      List<List<Object>> boundsList = dumpBounds.invoke();
      RTreeHelpers.dumpBoundsList(boundsList);
      List<List<Object>> allBoundsStats = boundsStats.invoke();
      RTreeHelpers.dumpLevelOverlapStats(allBoundsStats);
    }
  }


  public void loadTiger() throws Exception {

    try(InProcessCluster cluster = InProcessCluster.create()) {
      final RamaModule module = new Module();
      cluster.launchModule(module, new LaunchConfig(4, 3));

      final Depot depot = cluster.clusterDepot(Module.class.getName(), "*depot");
      final PState nodes = cluster.clusterPState(Module.class.getName(), "$$test__nodes");
      final PState root = cluster.clusterPState(Module.class.getName(), "$$test__root");
      final PState object = cluster.clusterPState(Module.class.getName(), "$$object");
      final PState objectLookup = cluster.clusterPState(Module.class.getName(), "$$objectLookup");
      final QueryTopologyClient q = cluster.clusterQuery(Module.class.getName(), "objectsInBounds");
      final QueryTopologyClient<Boolean> verify
          = cluster.clusterQuery(Module.class.getName(), "verifyTree");
      final QueryTopologyClient<List<String>> dumpDot
          = cluster.clusterQuery(Module.class.getName(), "dumpDot");
      final QueryTopologyClient<List<Long>> dump
          = cluster.clusterQuery(Module.class.getName(), "dumpTree");
      final QueryTopologyClient<List<List<Object>>> dumpBounds
          = cluster.clusterQuery(Module.class.getName(), "dumpBounds");

      final QueryTopologyClient<List<List<Object>>> boundsStats
          = cluster.clusterQuery(Module.class.getName(), "boundsStats");

      LOGGER.debug("START");

      // Shapefiles can be download from
      // https://www.census.gov/cgi-bin/geo/shapefiles/index.php
      //
      // Some datasets are Urban Areas (uac20), Places, Counties, and county
      // sub-divisions.
      File shapeFile = new File(
        new File(".").getAbsolutePath() +
        "/data/tiger/tl_2023_us_uac20/tl_2023_us_uac20.shp");

      int n = loadTigerFile(depot, shapeFile);

      cluster.waitForMicrobatchProcessedCount(
        module.getClass().getName(), "m", n);


      List<List<Object>> boundsList = dumpBounds.invoke();
      RTreeHelpers.dumpBoundsList(boundsList);
      List<List<Object>> allBoundsStats = boundsStats.invoke();
      RTreeHelpers.dumpLevelOverlapStats(allBoundsStats);
    }
  }

  private int loadTigerFile(final Depot depot, final File shapeFile)
      throws MalformedURLException, IOException {
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
    ShapefileDataStoreFactory dataStoreFactory
        = new ShapefileDataStoreFactory();
    ShapefileDataStore dataStore
        = (ShapefileDataStore) dataStoreFactory.createNewDataStore(params);
    // dataStore.createSchema(CITY);
    String nameAttribute = "NAME20";
    String typeName = dataStore.getTypeNames()[0];
    LOGGER.debug("typeName = " + typeName);
    LOGGER.debug("typeNames = " + dataStore.getTypeNames().length);
    FeatureSource<SimpleFeatureType, SimpleFeature> featureSource =
        dataStore.getFeatureSource(typeName);
    SimpleFeatureType schema = featureSource.getSchema();
    for (AttributeDescriptor attr : schema.getAttributeDescriptors()) {
      LOGGER.debug("Available attribute: " + attr.getLocalName());
    }
    FeatureCollection<SimpleFeatureType, SimpleFeature> collection =
        featureSource.getFeatures(// query
                                );
    // Iterate through features and get bounding boxes
    int i = 0;
    try (FeatureIterator<SimpleFeature> features = collection.features()) {
      while (features.hasNext()) {
        SimpleFeature feature = features.next();

        // Get the bounding box of the feature's geometry
        BoundingBox bounds = feature.getBounds();

        MBR mbr = new MBR(new double[]{bounds.getMinX(), bounds.getMinY()},
                          new double[]{bounds.getMaxX(), bounds.getMaxY()});

        depot.append(new AddObject(mbr, feature.getAttribute(nameAttribute)),
                     AckLevel.NONE);

        i = i + 1;
      }
    } catch (Exception e) {
      LOGGER.debug("Error: " + e);
    } finally {
      dataStore.dispose();
    }
    LOGGER.debug("Num objects: " + i);
    return i;
  }
}
