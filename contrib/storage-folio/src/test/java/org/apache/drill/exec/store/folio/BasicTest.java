package org.apache.drill.exec.store.folio;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import com.github.tomakehurst.wiremock.junit.WireMockRule;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

import org.apache.drill.shaded.guava.com.google.common.io.Resources;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.StoragePluginRegistryImpl;
import org.apache.drill.exec.store.folio.client.Raml;
import org.apache.drill.exec.store.folio.raml.ApiField;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.junit.Rule;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class BasicTest extends ClusterTest {

  @Rule
  public WireMockRule folioTestFixture = new WireMockRule(8081);

  @Test
  public void parseYamlToSchema() throws Exception {
    Yaml yaml = new Yaml();
    String document = "hello: 25";
    Map map = (Map) yaml.load(document);
    System.out.println(map.values().toArray()[0]);
    assertEquals("{hello=25}", map.toString());
  }

  @Test
  public void ApiFieldTest() {
    ApiField field1 = new ApiField("columnName", "type");
    System.out.println(field1.getName() + " " + field1.getType());

    ArrayList<ApiField> fields = new ArrayList<ApiField>();
    fields.add(field1);
  }

  @Test
  public void parseRamlToListOfFields() throws Exception {
    ArrayList<ApiField> fields = Raml.readSchemaFromRaml("ramls/location.raml", "location");
    for (ApiField f: fields) {
      System.out.println(f.getName() + ": " + f.getType());
    }
  }

  @Test
  public void selectAllWithDrillPlugin() throws Exception {

    startCluster(ClusterFixture.builder(dirTestWatcher));

    String url = "", tenant = "", username = "", password = "";
    try {
      JSONParser parser = new JSONParser();
      File jsonFile = new File(Resources.getResource("bootstrap-storage-plugins.json").toURI());
      Object obj = parser.parse(new FileReader(jsonFile));
      JSONObject bootstrap = (JSONObject) obj;
      JSONObject storage = (JSONObject) bootstrap.get("storage");
      JSONObject folioConfig = (JSONObject) storage.get("folio");
      url = (String) folioConfig.get("url");
      tenant = (String) folioConfig.get("tenant");
      username = (String) folioConfig.get("username");
      password = (String) folioConfig.get("password");

      FolioStorageConfig folioStorageConfig = new FolioStorageConfig(url, tenant, username, password);
      folioStorageConfig.setEnabled(true);

      String pluginName = "folio";
      DrillbitContext context = cluster.drillbit().getContext();
      FolioStoragePlugin folioStoragePlugin = new FolioStoragePlugin(folioStorageConfig,
          context, pluginName);

      StoragePluginRegistryImpl pluginRegistry = (StoragePluginRegistryImpl) context.getStorage();
      // pluginRegistry.addPluginToPersistentStoreIfAbsent(pluginName, folioStorageConfig, folioStoragePlugin);
      pluginRegistry.addEnabledPlugin(pluginName, folioStoragePlugin);
      pluginRegistry.createOrUpdate(pluginName, folioStorageConfig, true);

      String[] servicePointIds = {"3a40852d-49fd-4df2-a1f9-6e2641a6e91f"};
      LinkedHashMap<String, String> metadata = new LinkedHashMap<String, String>();
      metadata.put("createdDate", "2020-01-20T03:34:29.633+0000");
      metadata.put("updatedDate", "2020-01-20T03:34:29.633+0000");

      testBuilder()
        .sqlQuery("SELECT * FROM folio.locations")
        .unOrdered()
        .baselineColumns("id", "name", "code", "isActive", "institutionId", "campusId", "libraryId", "primaryServicePoint", "servicePointIds", "metadata")
        .baselineValues("758258bc-ecc1-41b8-abca-f7b610822ffd", "ORWIG ETHNO CD", "KU/CC/DI/O", true, "40ee00ca-a518-4b49-be01-0638d0a4ac57", "62cf76b7-cca5-4d33-9217-edf42ce1a848", "5d78803e-ca04-4b4a-aeae-2c63b924518b", "3a40852d-49fd-4df2-a1f9-6e2641a6e91f", servicePointIds, metadata)
        // .expectsNumRecords(7)
        // .baselineValues("xxx.xxx.xxx.xxx", "-", "GET /v1/yyy HTTP/1.1", "200", "412", "-", "Java/1.8.0_201", "3.580", "3.580", "api.example.com")
        .go();
      } catch (Exception e) {
        e.printStackTrace();
      }
  }

}