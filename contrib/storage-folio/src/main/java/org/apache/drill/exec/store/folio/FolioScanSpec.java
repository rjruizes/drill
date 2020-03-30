package org.apache.drill.exec.store.folio;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.z3950.zing.cql.CQLNode;

public class FolioScanSpec {

  private final String tableName;
  private CQLNode filters;

  @JsonCreator
  public FolioScanSpec(@JsonProperty("tableName") String tableName) {
    this.tableName = tableName;
  }
  public FolioScanSpec(String tableName, CQLNode filters) {
    this.tableName = tableName;
    this.filters = filters;
  }

  public CQLNode getFilters() {
    return filters;
  }

  public String getTableName() {
    return tableName;
  }

}
