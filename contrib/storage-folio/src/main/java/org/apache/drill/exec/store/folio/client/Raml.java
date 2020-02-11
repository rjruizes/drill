package org.apache.drill.exec.store.folio.client;

// import java.io.IOException;
// import java.util.ArrayList;
// import java.util.Iterator;
// import java.util.Map;
// import java.util.concurrent.ExecutionException;

// import com.fasterxml.jackson.core.JsonParseException;
// import com.github.jsonldjava.utils.JsonUtils;

// import org.apache.drill.exec.store.folio.raml.ApiField;
// import org.apache.drill.shaded.guava.com.google.common.io.Resources;

// import amf.client.model.domain.AnyShape;
// import webapi.Raml10;
// import webapi.WebApiDocument;

// public class Raml {

//   private static ApiField parseProperty(String name, Map<String, Object> property) {
//     String type = property.get("type").toString();

//     if(type.equals("object")) {
//       Map<String, Object> subProperties = (Map<String, Object>) property.get("properties");
//       ArrayList<ApiField> subFields = parseProperties(subProperties);
//       return new ApiField(name, type, subFields);
//     } else if(type.equals("array")) {
//       Map<String, Object> item = (Map<String, Object>) property.get("items");
//       ArrayList<ApiField> itemField = new ArrayList<ApiField>();
//       itemField.add(parseProperty("", item));
//       return new ApiField(name, type, itemField);
//     } else {
//       return new ApiField(name, type);
//     }
//   }

//   private static ArrayList<ApiField> parseProperties(Map<String, Object> properties) {
//     ArrayList<ApiField> fields = new ArrayList<ApiField>();
//     if(properties != null && properties.size() > 0) {
//       Iterator<Map.Entry<String, Object>> it = properties.entrySet().iterator();

//       while (it.hasNext()) {
//         Map.Entry<String, Object> pair = (Map.Entry<String, Object>)it.next();
//         Map<String, Object> val = (Map<String, Object>) pair.getValue();
//         String name = pair.getKey().toString();
//         ApiField field = parseProperty(name, val);
//         fields.add(field);
//         it.remove(); // avoids a ConcurrentModificationException
//       }
//     }
//     return fields;
//   }

//   public static ArrayList<ApiField> readSchemaFromRaml(String ramlFilepath, String collectionItemName)
//       throws InterruptedException, ExecutionException, JsonParseException, IOException {

//     String inp = Resources.getResource(ramlFilepath).toString();
//     WebApiDocument doc = (WebApiDocument) Raml10.parse(inp).get();
//     AnyShape typeInRoot = (AnyShape) doc.getDeclarationByName(collectionItemName);

//     String jsonSchema = typeInRoot.toJsonSchema();
//     Map<String, Object> jsonObject = (Map<String, Object>) JsonUtils.fromString(jsonSchema);
//     Map<String, Object> properties = (Map<String, Object>) jsonObject.get("properties");

//     return parseProperties(properties);
//   }
// }