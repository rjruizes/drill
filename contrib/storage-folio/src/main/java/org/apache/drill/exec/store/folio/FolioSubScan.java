package org.apache.drill.exec.store.folio;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableSet;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.physical.base.AbstractSubScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

@JsonTypeName("folio-sub-scan")
public class FolioSubScan extends AbstractSubScan {
  private final FolioStoragePlugin folioStoragePlugin;
  private final List<FolioSubScanSpec> scanSpecList;
  private final List<SchemaPath> columns;
  private int maxRecordsToRead;

  public FolioSubScan(@JacksonInject StoragePluginRegistry registry,
                      @JsonProperty("folioStoragePluginConfig") FolioStorageConfig folioStorageConfig,
                      @JsonProperty("tabletScanSpecList") LinkedList<FolioSubScanSpec> tabletScanSpecList,
                      @JsonProperty("columns") List<SchemaPath> columns,
                      @JsonProperty("maxRecordsToRead") int maxRecordsToRead) throws ExecutionSetupException {
    super((String) null);
    this.folioStoragePlugin = (FolioStoragePlugin) registry.getPlugin(folioStorageConfig);
    this.scanSpecList = tabletScanSpecList;
    this.columns = columns;
    this.maxRecordsToRead = maxRecordsToRead;
  }

  public FolioSubScan(FolioStoragePlugin plugin, List<FolioSubScanSpec> tabletInfoList, List<SchemaPath> columns,
      int maxRecordsToRead) {
    super((String) null);
    this.folioStoragePlugin = plugin;
    this.scanSpecList = tabletInfoList;
    this.columns = columns;
    this.maxRecordsToRead = maxRecordsToRead;
  }

  public FolioStorageConfig getFolioStoragePluginConfig() {
    return folioStoragePlugin.getConfig();
  }

  public List<FolioSubScanSpec> getTabletScanSpecList() {
    return scanSpecList;
  }

  @JsonProperty("columns")
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @JsonProperty("maxRecordsToRead")
  public int getMaxRecordsToRead() {
    return maxRecordsToRead;
  }

  @Override
  public boolean isExecutable() {
    return false;
  }

  @JsonIgnore
  public FolioStoragePlugin getStorageEngine(){
    return folioStoragePlugin;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }
  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new FolioSubScan(folioStoragePlugin, scanSpecList, columns, maxRecordsToRead);
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return ImmutableSet.<PhysicalOperator>of().iterator();
  }

  public static class FolioSubScanSpec {
    private final String tableName;

    @JsonCreator
    public FolioSubScanSpec(@JsonProperty("tableName") String tableName) {
      this.tableName = tableName;
    }

    public String getTableName() {
      return tableName;
    }
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.KUDU_SUB_SCAN_VALUE; // no other option
  }
}