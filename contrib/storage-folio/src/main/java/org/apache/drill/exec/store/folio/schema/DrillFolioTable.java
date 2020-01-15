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

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.folio.FolioStoragePlugin;
import org.apache.drill.exec.store.folio.raml.ApiField;
import org.apache.drill.exec.store.folio.FolioScanSpec;
// import org.apache.kudu.ColumnSchema;
// import org.apache.kudu.Schema;
// import org.apache.kudu.Type;

import org.apache.drill.shaded.guava.com.google.common.collect.Lists;

public class DrillFolioTable extends DynamicDrillTable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillFolioTable.class);

  private final ArrayList<ApiField> fields;

  public DrillFolioTable(String storageEngineName, FolioStoragePlugin plugin, ArrayList<ApiField> fields, FolioScanSpec scanSpec) {
    super(plugin, storageEngineName, scanSpec);
    this.fields = fields;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {

    List<String> names = Lists.newArrayList();
    List<RelDataType> types = Lists.newArrayList();
    for (ApiField field : fields) {
      names.add(field.getName());
      RelDataType type = getSqlTypeFromFolioType(typeFactory, field);
      type = typeFactory.createTypeWithNullability(type, type.isNullable());
      types.add(type);
    }

    return typeFactory.createStructType(types, names);
  }

  private RelDataType getSqlTypeFromFolioType(RelDataTypeFactory typeFactory, ApiField field) {
    switch (field.getType()) {
    case "boolean":
      return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
    case "object":
      ArrayList<RelDataType> types = new ArrayList<RelDataType>();
      ArrayList<String> names = new ArrayList<String>();
      for(ApiField f : field.getChildren()) {
        RelDataType dt = getSqlTypeFromFolioType(typeFactory, f);
        String name = f.getName();
        types.add(dt);
        names.add(name);
      }
      if(types.size() == 0) {
        return typeFactory.createUnknownType();
      }
      return typeFactory.createStructType(types, names);
    case "array":
      return typeFactory.createArrayType(getSqlTypeFromFolioType(typeFactory, field.getChildren().get(0)), -1);
    // case DOUBLE:
    //   return typeFactory.createSqlType(SqlTypeName.DOUBLE);
    // case FLOAT:
    //   return typeFactory.createSqlType(SqlTypeName.FLOAT);
    // case INT16:
    // case INT32:
    // case INT64:
    // case INT8:
    //   return typeFactory.createSqlType(SqlTypeName.INTEGER);
    case "string":
      return typeFactory.createSqlType(SqlTypeName.VARCHAR);
    // case UNIXTIME_MICROS:
    //   return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
    // case BINARY:
    //   return typeFactory.createSqlType(SqlTypeName.VARBINARY, Integer.MAX_VALUE);
    default:
      throw new UnsupportedOperationException("Unsupported type.");
    }
  }
}
