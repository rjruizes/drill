package org.apache.drill.exec.store.folio;

import java.io.File;
import java.io.FileReader;
import java.util.LinkedHashMap;

import com.github.tomakehurst.wiremock.junit.WireMockRule;

// import static com.github.tomakehurst.wiremock.client.WireMock.*;

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
  public void testLimit() throws Exception {
    testBuilder()
      .sqlQuery("SELECT id FROM folio.locations LIMIT 3")
      .unOrdered()
      .expectsNumRecords(3)
      .go();
  }

  @Test
  public void orderByIdAscending() throws Exception {
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
      .baselineValues("fcd64ce1-6995-48f0-840e-89ffa2288371")
      .go();
  }

  @Test
  public void orderByIdDescending() throws Exception {
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
  public void selectId() throws Exception {
    System.out.println("selectId");
    testBuilder()
      .sqlQuery("SELECT id FROM folio.locations WHERE id='758258bc-ecc1-41b8-abca-f7b610822ffd'")
      .unOrdered()
      .baselineColumns("id")
      .baselineValues("758258bc-ecc1-41b8-abca-f7b610822ffd")
      .go();
  }

  @Test
  public void selectAll() throws Exception {
    System.out.println("selectAll");
    final LinkedHashMap<String, String> metadata = new LinkedHashMap<String, String>();
    metadata.put("createdDate", "2020-01-20T03:34:29.633+0000");
    metadata.put("updatedDate", "2020-01-20T03:34:29.633+0000");

    testBuilder()
      .sqlQuery("SELECT * FROM folio.locations WHERE id='758258bc-ecc1-41b8-abca-f7b610822ffd'")
      .unOrdered()
      .baselineColumns("id", "name", "code", "isActive", "institutionId", "campusId",
      "libraryId", "primaryServicePoint", "servicePointIds", "metadata",
      "description", "details", "discoveryDisplayName")
      .baselineValues("758258bc-ecc1-41b8-abca-f7b610822ffd", "ORWIG ETHNO CD", "KU/CC/DI/O",
      "true", "40ee00ca-a518-4b49-be01-0638d0a4ac57", "62cf76b7-cca5-4d33-9217-edf42ce1a848",
      "5d78803e-ca04-4b4a-aeae-2c63b924518b", "3a40852d-49fd-4df2-a1f9-6e2641a6e91f",
      "[3a40852d-49fd-4df2-a1f9-6e2641a6e91f]", metadata.toString(),
      null, null, null)
      .go();
  }
}
