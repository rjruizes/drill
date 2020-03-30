package org.apache.drill.exec.store.folio.client;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Iterator;
// import java.util.List;
import java.util.Map;

import com.github.jsonldjava.utils.JsonUtils;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.utils.URIBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.z3950.zing.cql.CQLNode;

public class FolioScanner {
  static final Logger logger = LoggerFactory.getLogger(FolioScanner.class);
  private FolioClient client;
  private boolean hasMoreRows = true;
  // private List<String> projectedColumnNames;
  private String uri;

  public FolioScanner(String path, FolioClient client, int maxRecords, CQLNode filters)
      throws URISyntaxException, UnsupportedEncodingException {
    this.client = client;
    URIBuilder uriBuilder = client.getURI()
      .setPath(path.replaceAll("\\.", "/"))
      .addParameter("limit", String.valueOf(maxRecords));
    if(filters != null) {
      String q = filters.toCQL();
      uriBuilder.addParameter("query", q);
      System.out.printf("query: %s\n", q);
    }
    this.uri = uriBuilder.build().toString();

    System.out.printf("uri: %s\n", uri);
    logger.info("uri", uri);
  }

  public boolean hasMoreRows() {
    return hasMoreRows;
  }

  public Iterator<Map<String, Object>> nextRows() throws ClientProtocolException, IOException {
    String resp = client.get(uri);
    Map<String, Object> jsonResp = (Map<String, Object>) JsonUtils.fromString(resp);
    String recordsKey = "";
    for(String key : jsonResp.keySet()) {
      if(!key.equals("totalRecords")) {
        recordsKey = key;
        break;
      }
    }
    ArrayList<Map<String, Object>> records = (ArrayList<Map<String, Object>>) jsonResp.get(recordsKey);
    hasMoreRows = false;
    return records.iterator(); // TODO: this should concatenate to an iterator, not simply return a new one
  }

// public void setProjectedColumnNames(List<String> colNames) {
//   this.projectedColumnNames = colNames;
// }

}