package org.apache.drill.exec.store.folio;

import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.common.expression.LogicalExpression;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FolioFilterBuilder extends
    AbstractExprVisitor<FolioScanSpec, Void, RuntimeException> {
      static final Logger logger = LoggerFactory
      .getLogger(FolioFilterBuilder.class);
  final FolioGroupScan groupScan;
  final LogicalExpression le;
  private boolean allExpressionsConverted = true;

  public FolioFilterBuilder(FolioGroupScan groupScan,
      LogicalExpression conditionExp) {
    this.groupScan = groupScan;
    this.le = conditionExp;
  }

  public FolioScanSpec parseTree() {
    FolioScanSpec parsedSpec = le.accept(this, null);
    // if (parsedSpec != null) {
    //   parsedSpec = mergeScanSpecs("booleanAnd", this.groupScan.getFolioScanSpec(),
    //       parsedSpec);
    // }
    return parsedSpec;
  }

  private FolioScanSpec mergeScanSpecs(String functionName,
    FolioScanSpec leftScanSpec, FolioScanSpec rightScanSpec) {
    // Document newFilter = new Document();

    // switch (functionName) {
    // case "booleanAnd":
    //   if (leftScanSpec.getFilters() != null
    //       && rightScanSpec.getFilters() != null) {
    //     newFilter = MongoUtils.andFilterAtIndex(leftScanSpec.getFilters(),
    //         rightScanSpec.getFilters());
    //   } else if (leftScanSpec.getFilters() != null) {
    //     newFilter = leftScanSpec.getFilters();
    //   } else {
    //     newFilter = rightScanSpec.getFilters();
    //   }
    //   break;
    // case "booleanOr":
    //   newFilter = MongoUtils.orFilterAtIndex(leftScanSpec.getFilters(),
    //       rightScanSpec.getFilters());
    // }
    // return new FolioScanSpec(groupScan.getScanSpec().getDbName(), groupScan
    //     .getScanSpec().getCollectionName(), newFilter);
    return null;
  }

  public boolean isAllExpressionsConverted() {
    return allExpressionsConverted;
  }
}