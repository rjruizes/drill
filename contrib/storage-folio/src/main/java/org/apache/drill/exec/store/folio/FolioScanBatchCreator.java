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

import java.util.LinkedList;
import java.util.List;
import java.util.Collections;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.RecordReader;

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

public class FolioScanBatchCreator implements BatchCreator<FolioSubScan>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FolioScanBatchCreator.class);

  @Override
  public ScanBatch getBatch(ExecutorFragmentContext context, FolioSubScan subScan, List<RecordBatch> children)
      throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());
    List<RecordReader> readers = new LinkedList<>();
    List<SchemaPath> columns = null;
    int maxRecords = subScan.getMaxRecordsToRead();

    for (FolioSubScan.FolioSubScanSpec scanSpec : subScan.getTabletScanSpecList()) {
      try {
        if ((columns = subScan.getColumns())==null) {
          columns = GroupScan.ALL_COLUMNS;
        }
        readers.add(new FolioRecordReader(subScan.getStorageEngine().getClient(), scanSpec, columns, maxRecords));
      } catch (Exception e1) {
        throw new ExecutionSetupException(e1);
      }
    }
    // RecordReader reader = new FolioRecordReader(/*subScan.getStorageEngine().getClient(),*/ scanSpec, columns);
    return new ScanBatch(subScan, context, readers);
  }

}
