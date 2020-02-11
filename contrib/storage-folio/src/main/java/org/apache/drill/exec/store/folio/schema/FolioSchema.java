package org.apache.drill.exec.store.folio.schema;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.folio.FolioScanSpec;
import org.apache.drill.exec.store.folio.FolioStoragePlugin;
import org.apache.drill.exec.store.folio.FolioStorageConfig;

import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;

public class FolioSchema extends AbstractSchema {

  // private static final Logger logger = LoggerFactory.getLogger(FolioSchema.class);
  private final FolioStoragePlugin plugin;
  // private final Map<String, FolioSubSchema> schemaMap = Maps.newHashMap();
  private final Map<String, DrillTable> drillTables = Maps.newHashMap();
  // private Set<String> tableNames = new HashSet<>(Arrays.asList("locations", "example2"));

  public FolioSchema(final FolioStoragePlugin plugin, final String name) {
    super(ImmutableList.of(), name);
    this.plugin = plugin;
  }

  @Override
  public String getTypeName() {
    return FolioStorageConfig.NAME;
  }

  void setHolder(SchemaPlus plusOfThis) {
    for (String s : getSubSchemaNames()) {
      plusOfThis.add(s, getSubSchema(s));
    }
  }

  // @Override
  // public AbstractSchema getSubSchema(String name) {
  //   // List<String> tables;
  //   if (! schemaMap.containsKey(name)) {
  //     // tables = tableNameLoader.get(name);
  //     schemaMap.put(name, new FolioSubSchema(this, name));
  //   }

  //   return schemaMap.get(name);
  // }


  DrillTable getDrillTable(String tableName) {
    // MongoScanSpec mongoScanSpec = new MongoScanSpec(dbName, collectionName);
    // return new DynamicDrillTable(plugin, getName(), null, mongoScanSpec);
    FolioScanSpec scanSpec = new FolioScanSpec(tableName);
    return new DynamicDrillTable(plugin, tableName, null, scanSpec);
  }

  @Override
  public Table getTable(String tableName) {

    // if (!tableNames.contains(tableName)) { // table does not exist
    //   return null;
    // }

    if (! drillTables.containsKey(tableName)) {
      FolioScanSpec scanSpec = new FolioScanSpec(tableName);
      drillTables.put(tableName, new DynamicDrillTable(plugin, getName(), null, scanSpec));
    }

    return drillTables.get(tableName);
  }

  // @Override
  // public Set<String> getTableNames() {
  //   if (tableNames == null) {
  //     try {
  //       tableNames = new HashSet<>(Arrays.asList("locations", "example2"));
  //     } catch (Exception e) {
  //       logger.warn("Failure while loading table names for database '{}': {}", getName(), e.getMessage(), e.getCause());
  //       return Collections.emptySet();
  //     }
  //   }
  //   return tableNames;
  // }

  @Override
  public Set<String> getSubSchemaNames() {
    return Collections.emptySet();
  }
}