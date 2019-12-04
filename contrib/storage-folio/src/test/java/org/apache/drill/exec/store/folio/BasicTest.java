package org.apache.drill.exec.store.folio;

import java.io.File;
import java.io.FileReader;

import org.apache.drill.shaded.guava.com.google.common.io.Resources;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.StoragePluginRegistryImpl;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.junit.Test;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;


public class BasicTest extends ClusterTest {

  @Test
  public void getLucky() throws Exception {
    
    startCluster(ClusterFixture.builder(dirTestWatcher));

    String url = "", tenant = "", username = "", password = "";
    try {
      JSONParser parser = new JSONParser();
      File jsonFile = new File(Resources.getResource("test-config.json").toURI());
      Object obj = parser.parse(new FileReader(jsonFile));
      JSONObject jsonObject = (JSONObject) obj;
      url = (String) jsonObject.get("url");
      tenant = (String) jsonObject.get("tenant");
      username = (String) jsonObject.get("username");
      password = (String) jsonObject.get("password");
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }

    FolioStorageConfig folioStorageConfig = new FolioStorageConfig(url, tenant, username, password);
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