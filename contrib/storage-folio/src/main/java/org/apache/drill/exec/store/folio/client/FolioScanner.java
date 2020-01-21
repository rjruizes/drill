package org.apache.drill.exec.store.folio.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.github.jsonldjava.utils.JsonUtils;

import org.apache.http.client.ClientProtocolException;

public class FolioScanner {
  private String path;
  private FolioClient client;
  private boolean hasMoreRows = true;
  private List<String> projectedColumnNames;

  public FolioScanner(String path, FolioClient client) {
    this.path = path;
    this.client = client;
  }

  public boolean hasMoreRows() {
    return hasMoreRows;
  }

  public Iterator<Map<String, Object>> nextRows() throws ClientProtocolException, IOException {
    String resp = client.get(path); //  + "?limit=1"
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

public void setProjectedColumnNames(List<String> colNames) {
  this.projectedColumnNames = colNames;
}

}