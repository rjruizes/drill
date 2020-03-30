package org.apache.drill.exec.store.folio;

import org.z3950.zing.cql.*;

import org.apache.drill.common.expression.BooleanOperator;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.exec.store.folio.common.FolioCompareOp;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;

import java.io.IOException;

import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// FolioPushDownFilterForScan.java
// -> parseTree
//    - visitFunctionCall
//      - createFolioScanSpec: defines the list of valid filter operations
//      - mergeScanSpecs

public class FolioFilterBuilder extends
    AbstractExprVisitor<FolioScanSpec, Void, RuntimeException> {

  static final Logger logger = LoggerFactory.getLogger(FolioFilterBuilder.class);
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
    if (parsedSpec != null) {
      parsedSpec = mergeScanSpecs("booleanAnd", this.groupScan.getFolioScanSpec(),
          parsedSpec);
    }
    return parsedSpec;
  }

  private FolioScanSpec mergeScanSpecs(String functionName,
    FolioScanSpec leftScanSpec, FolioScanSpec rightScanSpec) {
    CQLNode newFilter = null;

    switch (functionName) {
    case "booleanAnd":
      if (leftScanSpec.getFilters() != null
          && rightScanSpec.getFilters() != null) {
        newFilter = new CQLAndNode(leftScanSpec.getFilters(), rightScanSpec.getFilters(), new ModifierSet("and"));
      } else if (leftScanSpec.getFilters() != null) {
        newFilter = leftScanSpec.getFilters();
      } else {
        newFilter = rightScanSpec.getFilters();
      }
      break;
    case "booleanOr":
      if (leftScanSpec.getFilters() != null
          && rightScanSpec.getFilters() != null) {
        newFilter = new CQLOrNode(leftScanSpec.getFilters(), rightScanSpec.getFilters(), new ModifierSet("or"));
      } else if (leftScanSpec.getFilters() != null) {
        newFilter = leftScanSpec.getFilters();
      } else {
        newFilter = rightScanSpec.getFilters();
      }
      break;
    }
    return new FolioScanSpec(groupScan.getFolioScanSpec().getTableName(), newFilter);
  }

  public boolean isAllExpressionsConverted() {
    return allExpressionsConverted;
  }

  @Override
  public FolioScanSpec visitBooleanOperator(BooleanOperator op, Void value) throws RuntimeException {
    return visitFunctionCall(op, value);
  }

  @Override
  public FolioScanSpec visitFunctionCall(FunctionCall call, Void value)
      throws RuntimeException {
    FolioScanSpec nodeScanSpec = null;
    String functionName = call.getName();
    ImmutableList<LogicalExpression> args = call.args;

    if (FolioCompareFunctionProcessor.isCompareFunction(functionName)) {
      FolioCompareFunctionProcessor processor = FolioCompareFunctionProcessor
          .process(call);
      if (processor.isSuccess()) {
        try {
          nodeScanSpec = createFolioScanSpec(processor.getFunctionName(),
              processor.getPath(), processor.getValue());
        } catch (Exception e) {
          logger.error(" Failed to create Filter ", e);
          // throw new RuntimeException(e.getMessage(), e);
        }
      }
    } else {
      switch (functionName) {
      case "booleanAnd":
      case "booleanOr":
        FolioScanSpec leftScanSpec = args.get(0).accept(this, null);
        FolioScanSpec rightScanSpec = args.get(1).accept(this, null);
        if (leftScanSpec != null && rightScanSpec != null) {
          nodeScanSpec = mergeScanSpecs(functionName, leftScanSpec,
              rightScanSpec);
        } else {
          allExpressionsConverted = false;
          if ("booleanAnd".equals(functionName)) {
            nodeScanSpec = leftScanSpec == null ? rightScanSpec : leftScanSpec;
          }
        }
        break;
      }
    }

    if (nodeScanSpec == null) {
      allExpressionsConverted = false;
    }

    return nodeScanSpec;
  }


  private FolioScanSpec createFolioScanSpec(String functionName,
      SchemaPath field, Object fieldValue) throws ClassNotFoundException,
      IOException {
    // extract the field name
    String fieldName = field.getRootSegmentPath();
    FolioCompareOp compareOp = null;
    switch (functionName) {
    case "equal":
      compareOp = FolioCompareOp.EQUAL;
      break;
    case "not_equal":
      compareOp = FolioCompareOp.NOT_EQUAL;
      break;
    case "greater_than_or_equal_to":
      compareOp = FolioCompareOp.GREATER_OR_EQUAL;
      break;
    case "greater_than":
      compareOp = FolioCompareOp.GREATER;
      break;
    case "less_than_or_equal_to":
      compareOp = FolioCompareOp.LESS_OR_EQUAL;
      break;
    case "less_than":
      compareOp = FolioCompareOp.LESS;
      break;
    // case "isnull":
    // case "isNull":
    // case "is null":
    //   compareOp = FolioCompareOp.IFNULL;
    //   break;
    // case "isnotnull":
    // case "isNotNull":
    // case "is not null":
    //   compareOp = FolioCompareOp.IFNOTNULL;
    //   break;
    }

    if (compareOp != null) {
      // if (compareOp == FolioCompareOp.IFNULL) {
      //   new CQLTermNode(fieldName, new CQLRelation("="), "");
      //   queryFilter.put(fieldName,
      //       new Filter(FolioCompareOp.EQUAL.getCompareOp(), null));
      // } else if (compareOp == FolioCompareOp.IFNOTNULL) {
      //   new CQLNotNode(left, right, ms);
      //   queryFilter.put(fieldName,
      //       new Filter(FolioCompareOp.NOT_EQUAL.getCompareOp(), null));
      // } else {
      //   queryFilter.put(fieldName, new Filter(compareOp.getCompareOp(),
      //       fieldValue));
      // }
      CQLNode cql = new CQLTermNode(fieldName, new CQLRelation(compareOp.getCompareOp()), fieldValue.toString());
      return new FolioScanSpec(groupScan.getFolioScanSpec().getTableName(), cql); // queryFilter
    }
    return null;
  }

}