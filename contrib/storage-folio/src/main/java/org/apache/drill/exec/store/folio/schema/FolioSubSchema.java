/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.folio.schema;

import java.util.Map;

import org.apache.drill.shaded.guava.com.google.common.collect.Maps;
import org.apache.calcite.schema.Table;

import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.folio.FolioStorageConfig;

public class FolioSubSchema extends AbstractSchema {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory
      .getLogger(FolioSubSchema.class);
  private final FolioSchema folioSchema;
  // private final Set<String> tableNames;

  private final Map<String, DrillTable> drillTables = Maps.newHashMap();

  public FolioSubSchema(FolioSchema folioSchema, String name) {
    super(folioSchema.getSchemaPath(), name);
    this.folioSchema = folioSchema;
    // this.tableNames = Sets.newHashSet(tableList);
  }
  public static String removePrefix(String s, String prefix) {
    if (s != null && prefix != null && s.startsWith(prefix)){
      return s.substring(prefix.length());
    }
    return s;
  }
  @Override
  public Table getTable(String tableName) {
    String fullName = removePrefix(this.getFullSchemaName(), "folio.") + "." + tableName;
    // if (!tableNames.contains(tableName)) { // table does not exist
    //   return null;
    // }

    if (! drillTables.containsKey(fullName)) {
      drillTables.put(fullName, folioSchema.getDrillTable(fullName));
    }

    return drillTables.get(fullName);

  }

  // @Override
  // public Set<String> getTableNames() {
  //   return tableNames;
  // }

  @Override
  public String getTypeName() {
    return FolioStorageConfig.NAME;
  }

}
