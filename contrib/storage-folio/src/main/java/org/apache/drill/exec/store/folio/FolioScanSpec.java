package org.apache.drill.exec.store.folio;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class FolioScanSpec {

  private final String tableName;
  private Filter filters;

  @JsonCreator
  public FolioScanSpec(@JsonProperty("tableName") String tableName) {
    this.tableName = tableName;
  }
  public FolioScanSpec(String tableName, Filter filters) {
    this.tableName = tableName;
    this.filters = filters;
  }

  public Filter getFilters() {
    return filters;
  }

  public String getTableName() {
    return tableName;
  }

}
