package org.apache.drill.exec.store.folio;

import static org.junit.Assert.assertEquals;

// import org.z3950.zing.cql.*;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class CqlTest {
  @Test
  public void simpleQuery() throws MalformedURLException {
    URL basicUrl = new URL("https://example.com/inventory?query=a&sort=Title");
    String query = basicUrl.getQuery();
    Map<String, String> map = getQueryMap(query);
    System.out.println(map.get("query") );
  }
  @Test
  public void complexQuery() throws MalformedURLException {
    URL basicUrl = new URL("https://example.com/inventory?limit=100&query=%28keyword%20all%20%22a%22%29%20sortby%20title%2Fsort.descending");
    String query = basicUrl.getQuery();
    Map<String, String> map = getQueryMap(query);
    System.out.println(map);
  }

  @Test
  public void testCql() {
    // in: ORDER BY Country DESC
    // out: sortby title/sort.descending
    // CQLNode term = new CQLTermNode("keyword", new CQLRelation("all"), "a");
    // CQLNode sort = new CQLSortNode(subtree)
    // CQLNode root = new CQLAndNode(term, sort);
  }

  @Test
  public void simpleSql() {
    String inputSql = "SELECT * FROM Customers ORDER BY Country DESC";
    // expected output = "sortby=-Country"
  }
  public String sqlToCql(String sql) {

    return "";
  }
  public Map<String, String> getQueryMap(String query) {
    String[] params = query.split("&");
    Map<String, String> map = new HashMap<String, String>();
    for (String param : params)
    {
        String name = param.split("=")[0];
        String value = param.split("=")[1];
        map.put(name, value);
    }
    return map;
  }
}