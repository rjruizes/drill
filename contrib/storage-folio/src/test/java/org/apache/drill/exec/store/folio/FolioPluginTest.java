package org.apache.drill.exec.store.folio;

import java.io.File;
import java.io.FileReader;
import java.util.LinkedHashMap;

import com.github.tomakehurst.wiremock.junit.WireMockRule;

import org.apache.drill.shaded.guava.com.google.common.io.Resources;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class FolioPluginTest extends ClusterTest {

  @Rule
  public WireMockRule folioTestFixture = new WireMockRule(8081);

  @BeforeClass
  public static void setup() throws Exception {

    startCluster(ClusterFixture.builder(dirTestWatcher));

    String url = "", tenant = "", username = "", password = "";
    try {
      final JSONParser parser = new JSONParser();
      final File jsonFile = new File(Resources.getResource("bootstrap-storage-plugins.json").toURI());
      final Object obj = parser.parse(new FileReader(jsonFile));
      final JSONObject bootstrap = (JSONObject) obj;
      final JSONObject storage = (JSONObject) bootstrap.get("storage");
      final JSONObject folioConfig = (JSONObject) storage.get("folio");
      url = (String) folioConfig.get("url");
      tenant = (String) folioConfig.get("tenant");
      username = (String) folioConfig.get("username");
      password = (String) folioConfig.get("password");

      final FolioStorageConfig folioStorageConfig = new FolioStorageConfig(url, tenant, username, password);
      folioStorageConfig.setEnabled(true);

      final String pluginName = "folio";

      final Drillbit drillbit = cluster.drillbit();
      final StoragePluginRegistry pluginRegistry = drillbit.getContext().getStorage();

      final FolioStoragePlugin folioStoragePlugin = new FolioStoragePlugin(folioStorageConfig,
          drillbit.getContext(), pluginName);

      // pluginRegistry.addPluginToPersistentStoreIfAbsent(pluginName, folioStorageConfig, folioStoragePlugin);
      pluginRegistry.addEnabledPlugin(pluginName, folioStoragePlugin);
      pluginRegistry.createOrUpdate(pluginName, folioStorageConfig, true);
    } catch (final Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void inventoryItemsTestLimit() throws Exception {
    testBuilder()
      .sqlQuery("SELECT `id` FROM folio.`inventory/items` LIMIT 3")
      .unOrdered()
      .expectsNumRecords(3)
      .go();
  }

  @Test
  public void locationsTestLimit() throws Exception {
    testBuilder()
      .sqlQuery("SELECT `id` FROM folio.locations LIMIT 3")
      .unOrdered()
      .expectsNumRecords(3)
      .go();
  }

// The ORDER BY tests fail in live instances because the IDs don't match

  @Test
  public void locationsTestOrderByIdAscending() throws Exception {
    testBuilder()
      .sqlQuery("SELECT id FROM folio.locations ORDER BY id ASC")
      .unOrdered()
      .baselineColumns("id")
      .baselineValues("184aae84-a5bf-4c6a-85ba-4a7c73026cd5")
      .baselineValues("31697c89-4164-4447-b5b0-b38870712e0f")
      .baselineValues("53cf956f-c1df-410b-8bea-27f712cca7c0")
      .baselineValues("758258bc-ecc1-41b8-abca-f7b610822ffd")
      .baselineValues("b241764c-1466-4e1d-a028-1a3684a5da87")
      .baselineValues("f34d27c6-a8eb-461b-acd6-5dea81771e70")
      // .baselineValues("fcd64ce1-6995-48f0-840e-89ffa2288371")
      .go();
  }

  @Test
  public void locationsTestOrderByIdDescending() throws Exception {
    testBuilder()
      .sqlQuery("SELECT id FROM folio.locations ORDER BY id DESC")
      .unOrdered()
      .baselineColumns("id")
      .baselineValues("fcd64ce1-6995-48f0-840e-89ffa2288371")
      .baselineValues("f34d27c6-a8eb-461b-acd6-5dea81771e70")
      .baselineValues("b241764c-1466-4e1d-a028-1a3684a5da87")
      .baselineValues("758258bc-ecc1-41b8-abca-f7b610822ffd")
      .baselineValues("53cf956f-c1df-410b-8bea-27f712cca7c0")
      .baselineValues("31697c89-4164-4447-b5b0-b38870712e0f")
      .baselineValues("184aae84-a5bf-4c6a-85ba-4a7c73026cd5")
      .go();
  }

  @Test
  public void locationsTestSelectIdWhereId() throws Exception {
    testBuilder()
      .sqlQuery("SELECT id FROM folio.locations WHERE id='758258bc-ecc1-41b8-abca-f7b610822ffd'")
      .unOrdered()
      .baselineColumns("id")
      .baselineValues("758258bc-ecc1-41b8-abca-f7b610822ffd")
      .go();
  }

  @Test
  public void locationsTestAnd() throws Exception {
    String query = String.format("%s %s %s",
    "SELECT id, name FROM folio.locations",
    "WHERE id='758258bc-ecc1-41b8-abca-f7b610822ffd'",
    "AND name='ORWIG ETHNO CD'");
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("id", "name")
      .baselineValues("758258bc-ecc1-41b8-abca-f7b610822ffd", "ORWIG ETHNO CD")
      .go();
  }

  @Test
  public void locationsTestOr() throws Exception {
    String query = String.format("%s %s %s",
    "SELECT id, name FROM folio.locations",
    "WHERE id='758258bc-ecc1-41b8-abca-f7b610822ffd'",
    "OR name='Main Library'");
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("id", "name")
      .baselineValues("758258bc-ecc1-41b8-abca-f7b610822ffd", "ORWIG ETHNO CD")
      .baselineValues("fcd64ce1-6995-48f0-840e-89ffa2288371", "Main Library")
      .go();
  }

  @Test
  public void locationsTestWhereId() throws Exception {
    final LinkedHashMap<String, String> metadata = new LinkedHashMap<String, String>();
    metadata.put("createdDate", "2020-01-20T03:34:29.633+0000");
    metadata.put("updatedDate", "2020-01-20T03:34:29.633+0000");

    testBuilder()
      .sqlQuery("SELECT id, name, institutionId FROM folio.locations WHERE id='758258bc-ecc1-41b8-abca-f7b610822ffd'")
      .unOrdered()
      .baselineColumns("id", "name", "institutionId")
      .baselineValues("758258bc-ecc1-41b8-abca-f7b610822ffd", "ORWIG ETHNO CD",
      "40ee00ca-a518-4b49-be01-0638d0a4ac57")
      .go();
  }
}
