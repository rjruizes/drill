package org.apache.drill.exec.store.folio.schema;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.folio.FolioStoragePlugin;
import org.apache.drill.exec.store.folio.FolioStorageConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;

public class FolioSchema extends AbstractSchema {

  private static final Logger logger = LoggerFactory.getLogger(FolioSchema.class);
  private final FolioStoragePlugin plugin;
  private final Map<String, DrillTable> drillTables = Maps.newHashMap();
  private Set<String> tableNames;

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
  // public Table getTable(String tableName) {
  //   if (!drillTables.containsKey(tableName)) {
  //     KafkaScanSpec scanSpec = new KafkaScanSpec(tableName);
  //     DrillTable table = new DynamicDrillTable(plugin, getName(), scanSpec);
  //     drillTables.put(tableName, table);
  //   }

  //   return drillTables.get(tableName);
  // }

  // @Override
  // public Set<String> getTableNames() {
  //   if (tableNames == null) {
  //     try (KafkaConsumer<?, ?> kafkaConsumer = new KafkaConsumer<>(plugin.getConfig().getKafkaConsumerProps())) {
  //       tableNames = kafkaConsumer.listTopics().keySet();
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