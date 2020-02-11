package org.apache.drill.exec.store.folio.client;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
// import java.util.List;
import java.util.Map;

import com.github.jsonldjava.utils.JsonUtils;

import org.apache.drill.exec.store.folio.Filter;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.utils.URIBuilder;

public class FolioScanner {
  private FolioClient client;
  private boolean hasMoreRows = true;
  // private List<String> projectedColumnNames;
  private String uri;

  public FolioScanner(String path, FolioClient client, int maxRecords, Filter filters) throws URISyntaxException {
    this.client = client;
    URIBuilder uriBuilder = client.getURI()
      .setPath(path.replaceAll("\\.", "/"))
      .addParameter("limit", String.valueOf(maxRecords));
    if(filters != null) {
      uriBuilder.addParameter("query", filters.toCql());
    }
    this.uri = uriBuilder.build().toString();
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