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
package org.apache.drill.exec.store.folio;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.github.jsonldjava.utils.JsonUtils;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.folio.FolioSubScan.FolioSubScanSpec;
import org.apache.drill.exec.store.folio.client.FolioClient;
import org.apache.drill.exec.store.folio.client.FolioScanner;
import org.apache.drill.exec.store.folio.raml.ApiField;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.BitVector;
import org.apache.drill.exec.vector.Float4Vector;
import org.apache.drill.exec.vector.Float8Vector;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.NullableBitVector;
import org.apache.drill.exec.vector.NullableFloat4Vector;
import org.apache.drill.exec.vector.NullableFloat8Vector;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.NullableTimeStampVector;
import org.apache.drill.exec.vector.NullableVarBinaryVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.TimeStampVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarBinaryVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableMap;

import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;

public class FolioRecordReader extends AbstractRecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FolioRecordReader.class);

  private static final int TARGET_RECORD_COUNT = 4000;

  private final FolioClient client;
  private final FolioSubScanSpec scanSpec;
  private FolioScanner scanner;
  private Iterator<Map<String, Object>> iterator;

  private OutputMutator output;
  private OperatorContext context;
  private boolean hasBeenRead = false;

  private static class ProjectedColumnInfo {
    String index;
    ValueVector vv;
  }

  private ImmutableList<ProjectedColumnInfo> projectedCols;

  public FolioRecordReader(FolioClient client, FolioSubScan.FolioSubScanSpec subScanSpec, List<SchemaPath> projectedColumns) {
    setColumns(projectedColumns);
    this.client = client;
    scanSpec = subScanSpec;
    logger.debug("Scan spec: {}", subScanSpec);
  }

  @Override
  public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
    this.output = output;
    this.context = context;
    try {
      scanner = new FolioScanner(scanSpec.getTableName(), client);
      
      System.out.println("tableName: " + scanSpec.getTableName());
      if(!isStarQuery()) {
        System.out.println("Is NOT a star query");
        List<String> colNames = Lists.newArrayList();
        for (SchemaPath p : this.getColumns()) {
          colNames.add(p.getRootSegmentPath());
        }
        scanner.setProjectedColumnNames(colNames);
      }
    } catch (Exception e) {
      throw new ExecutionSetupException(e);
    }
  }

  @Override
  public int next() {
    int rowCount = 0;
    try {
      while (iterator == null || !iterator.hasNext()) {
        if (!scanner.hasMoreRows()) {
          iterator = null;
          return 0;
        }
        iterator = scanner.nextRows();
      }
      for (; rowCount < TARGET_RECORD_COUNT && iterator.hasNext(); rowCount++) {
        addRowResult(iterator.next(), rowCount);
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    for (ProjectedColumnInfo pci : projectedCols) {
      pci.vv.getMutator().setValueCount(rowCount);
    }
    return rowCount;
  }

  private void initCols(Map<String, Object> record) throws SchemaChangeException {
    ImmutableList.Builder<ProjectedColumnInfo> pciBuilder = ImmutableList.builder();

    for (Map.Entry<String, Object> entry : record.entrySet()) {

      final String name = entry.getKey();
      final Object val = entry.getValue();
      MinorType minorType = MinorType.VARCHAR;
      if(val instanceof String) {
        minorType = MinorType.VARCHAR;
      }
      
      MajorType majorType = Types.optional(minorType);
      MaterializedField field = MaterializedField.create(name, majorType);
      final Class<? extends ValueVector> clazz = TypeHelper.getValueVectorClass(
          minorType, majorType.getMode());
      ValueVector vector = output.addField(field, clazz);
      vector.allocateNew();

      ProjectedColumnInfo pci = new ProjectedColumnInfo();
      pci.vv = vector;
      pci.index = name;
      System.out.println(name);
      pciBuilder.add(pci);
    }

    projectedCols = pciBuilder.build();
  }

  private void addRowResult(Map<String, Object> record, int rowIndex) throws SchemaChangeException {
    if (projectedCols == null) {
      initCols(record);
    }

    for (ProjectedColumnInfo pci : projectedCols) {
      ByteBuffer value = ByteBuffer.wrap(record.get(pci.index).toString().getBytes());
      
      ((NullableVarCharVector.Mutator) pci.vv.getMutator())
        .setSafe(rowIndex, value, 0, value.remaining());
    }
  }

  @Override
  public void close() {
  }

  @Override
  public String toString() {
    return "FolioRecordReader["
        + "]";
  }
}
