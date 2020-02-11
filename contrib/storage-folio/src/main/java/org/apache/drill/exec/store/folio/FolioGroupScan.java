package org.apache.drill.exec.store.folio;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.ScanStats.GroupScanProperty;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.folio.FolioSubScan.FolioSubScanSpec;
import org.apache.drill.exec.store.schedule.AffinityCreator;
import org.apache.drill.exec.store.schedule.CompleteWork;
import org.apache.drill.exec.store.schedule.EndpointByteMap;
import org.apache.drill.exec.store.schedule.EndpointByteMapImpl;
import org.apache.drill.common.expression.SchemaPath;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("folio-scan")
public class FolioGroupScan extends AbstractGroupScan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FolioGroupScan.class);

  private FolioStoragePlugin folioStoragePlugin;
  private List<SchemaPath> columns;
  private FolioScanSpec folioScanSpec;
  private static final long DEFAULT_TABLET_SIZE = 1000;
  private List<EndpointAffinity> affinities;
  private List<FolioWork> folioWorkList = Lists.newArrayList();
  private boolean filterPushedDown = false;
  protected int maxRecordsToRead = 100;

  @JsonCreator
  public FolioGroupScan(@JsonProperty("folioScanSpec") FolioScanSpec folioScanSpec,
                        @JsonProperty("folioStoragePluginConfig") FolioStorageConfig folioStorageConfig,
                        @JsonProperty("columns") List<SchemaPath> columns,
                        @JacksonInject StoragePluginRegistry pluginRegistry) throws IOException, ExecutionSetupException {
    this((FolioStoragePlugin) pluginRegistry.getPlugin(folioStorageConfig), folioScanSpec, columns);
  }

  public FolioGroupScan(FolioStoragePlugin folioStoragePlugin,
                       FolioScanSpec folioScanSpec,
                       List<SchemaPath> columns) {
    super((String) null);
    this.folioStoragePlugin = folioStoragePlugin;
    this.folioScanSpec = folioScanSpec;
    this.columns = columns == null || columns.size() == 0? ALL_COLUMNS : columns;
    init();
  }

  /**
   * Checks if Json table reader supports limit push down.
   *
   * @return true if limit push down is supported, false otherwise
   */
  @Override
  public boolean supportsLimitPushdown() {
    if (maxRecordsToRead == 100) {
      return true;
    }
    return false; // limit is already pushed. No more pushdown of limit
  }

  @Override
  public GroupScan applyLimit(int maxRecords) {
    maxRecordsToRead = Math.max(maxRecords, 1);
    return this;
  }

  @Override
  @JsonIgnore
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    return true;
  }

  @JsonIgnore
  public boolean isFilterPushedDown() {
    return filterPushedDown;
  }

  @JsonIgnore
  public void setFilterPushedDown(boolean filterPushedDown) {
    this.filterPushedDown = filterPushedDown;
  }

  private void init() {
    // String tableName = folioScanSpec.getTableName();
    Collection<DrillbitEndpoint> endpoints = folioStoragePlugin.getContext().getBits();
    Map<String,DrillbitEndpoint> endpointMap = Maps.newHashMap();
    for (DrillbitEndpoint endpoint : endpoints) {
      endpointMap.put(endpoint.getAddress(), endpoint);
    }
    try {
      // List<LocatedTablet> locations = folioStoragePlugin.getClient().openTable(tableName).getTabletsLocations(10000);
      // for (LocatedTablet tablet : locations) {
      //   FolioWork work = new FolioWork(tablet.getPartition().getPartitionKeyStart(), tablet.getPartition().getPartitionKeyEnd());
      //   for (Replica replica : tablet.getReplicas()) {
      //     String host = replica.getRpcHost();
      //     DrillbitEndpoint ep = endpointMap.get(host);
      //     if (ep != null) {
      //       work.getByteMap().add(ep, DEFAULT_TABLET_SIZE);
      //     }
      //   }
      //   folioWorkList.add(work);
      // }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  private static class FolioWork implements CompleteWork {

    private EndpointByteMapImpl byteMap = new EndpointByteMapImpl();
    // private byte[] partitionKeyStart;
    // private byte[] partitionKeyEnd;

    // public FolioWork(byte[] partitionKeyStart, byte[] partitionKeyEnd) {
    //   this.partitionKeyStart = partitionKeyStart;
    //   this.partitionKeyEnd = partitionKeyEnd;
    // }

    // public byte[] getPartitionKeyStart() {
    //   return partitionKeyStart;
    // }

    // public byte[] getPartitionKeyEnd() {
    //   return partitionKeyEnd;
    // }

    @Override
    public long getTotalBytes() {
      return DEFAULT_TABLET_SIZE;
    }

    @Override
    public EndpointByteMap getByteMap() {
      return byteMap;
    }

    @Override
    public int compareTo(CompleteWork o) {
      return 0;
    }
  }

  /**
   * Private constructor, used for cloning.
   * @param that The FolioGroupScan to clone
   */
  private FolioGroupScan(FolioGroupScan that) {
    super(that);
    this.folioStoragePlugin = that.folioStoragePlugin;
    this.columns = that.columns;
    this.folioScanSpec = that.folioScanSpec;
    this.filterPushedDown = that.filterPushedDown;
    this.maxRecordsToRead = that.maxRecordsToRead;
    // this.folioWorkList = that.folioWorkList;
    // this.assignments = that.assignments;
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    FolioGroupScan newScan = new FolioGroupScan(this);
    newScan.columns = columns;
    return newScan;
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    if (affinities == null) {
      affinities = AffinityCreator.getAffinityMap(folioWorkList);
    }
    return affinities;
  }

  @Override
  public ScanStats getScanStats() {
    return new ScanStats(
        GroupScanProperty.EXACT_ROW_COUNT,
        1, // TODO: 17 is arbitary
        1,
        1);
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints) throws PhysicalOperatorSetupException {
    // TODO Auto-generated method stub

  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException {
    // List<KuduWork> workList = assignments.get(minorFragmentId);
    List<FolioSubScanSpec> scanSpecList = Lists.newArrayList();
    // for (KuduWork work : workList) {
    //   scanSpecList.add(new KuduSubScanSpec(getTableName(), work.getPartitionKeyStart(), work.getPartitionKeyEnd()));
    // }
    scanSpecList.add(new FolioSubScanSpec(getTableName(), getFilters()));
    return new FolioSubScan(folioStoragePlugin, scanSpecList, this.columns, this.maxRecordsToRead);
  }

  @Override
  public int getMaxParallelizationWidth() {
    // TODO Auto-generated method stub
    return 0;
  }

  @JsonIgnore
  public String getTableName() {
    return getFolioScanSpec().getTableName();
  }
  @JsonIgnore
  public Filter getFilters() {
    return getFolioScanSpec().getFilters();
  }

  @JsonIgnore
  public FolioStoragePlugin getStoragePlugin() {
    return folioStoragePlugin;
  }

  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  public String toString() {
    return "FolioGroupScan [FolioScanSpec="
        + folioScanSpec + ", columns="
        + columns + "]";
  }

  @JsonProperty
  public FolioScanSpec getFolioScanSpec() {
    return folioScanSpec;
  }

  @Override
  @JsonIgnore
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new FolioGroupScan(this);
  }

}