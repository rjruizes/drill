package org.apache.drill.exec.store.folio;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class FolioScanSpec {

  private final String tableName;

  @JsonCreator
  public FolioScanSpec(@JsonProperty("tableName") String tableName) {
    this.tableName = tableName;
  }

  public String getTableName() {
    return tableName;
  }

}
