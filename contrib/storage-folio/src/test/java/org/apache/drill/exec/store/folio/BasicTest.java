package org.apache.drill.exec.store.folio;

import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.StoragePluginRegistryImpl;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.junit.Test;


public class BasicTest extends ClusterTest {

  @Test
  public void getLucky() throws Exception {

    
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "trace");
    
    startCluster(ClusterFixture.builder(dirTestWatcher));

    FolioStorageConfig folioStorageConfig = new FolioStorageConfig(
        "localhost",
        "diku",
        "");
    folioStorageConfig.setEnabled(true);

    String pluginName = "folio";
    DrillbitContext context = cluster.drillbit().getContext();
    FolioStoragePlugin folioStoragePlugin = new FolioStoragePlugin(folioStorageConfig,
        context, pluginName);

    StoragePluginRegistryImpl pluginRegistry = (StoragePluginRegistryImpl) context.getStorage();
    pluginRegistry.addPluginToPersistentStoreIfAbsent(pluginName, folioStorageConfig, folioStoragePlugin);

    testBuilder()
      .sqlQuery("SELECT * FROM folio.locations")
      .unOrdered()
      .baselineColumns("col0", "col1")
      .baselineValues("val", "val")
      // .baselineValues("xxx.xxx.xxx.xxx", "-", "GET /v1/yyy HTTP/1.1", "200", "412", "-", "Java/1.8.0_201", "3.580", "3.580", "api.example.com")
      .go();
  }

}