package org.apache.drill.exec.store.folio;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Map;

import org.apache.drill.exec.store.folio.client.Raml;
import org.apache.drill.exec.store.folio.raml.ApiField;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

public class BasicTest {

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
}