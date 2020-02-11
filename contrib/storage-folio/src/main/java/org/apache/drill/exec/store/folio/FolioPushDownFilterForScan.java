package org.apache.drill.exec.store.folio;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rex.RexNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;

public class FolioPushDownFilterForScan extends StoragePluginOptimizerRule {
  private static final Logger logger = LoggerFactory
      .getLogger(FolioPushDownFilterForScan.class);
  public static final StoragePluginOptimizerRule INSTANCE = new FolioPushDownFilterForScan();

  private FolioPushDownFilterForScan() {
    super(
        RelOptHelper.some(FilterPrel.class, RelOptHelper.any(ScanPrel.class)),
        "FolioPushDownFilterForScan");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final ScanPrel scan = (ScanPrel) call.rel(1);
    final FilterPrel filter = (FilterPrel) call.rel(0);
    final RexNode condition = filter.getCondition();

    FolioGroupScan groupScan = (FolioGroupScan) scan.getGroupScan();
    if (groupScan.isFilterPushedDown()) {
      return;
    }

    LogicalExpression conditionExp = DrillOptiq.toDrill(
        new DrillParseContext(PrelUtil.getPlannerSettings(call.getPlanner())), scan, condition);
    FolioFilterBuilder folioFilterBuilder = new FolioFilterBuilder(groupScan,
        conditionExp);
    FolioScanSpec newScanSpec = folioFilterBuilder.parseTree();
    if (newScanSpec == null) {
      return; // no filter pushdown so nothing to apply.
    }

    FolioGroupScan newGroupsScan = null;
    try {
      newGroupsScan = new FolioGroupScan(groupScan.getStoragePlugin(),
          newScanSpec, groupScan.getColumns());
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      throw new DrillRuntimeException(e.getMessage(), e);
    }
    newGroupsScan.setFilterPushedDown(true);

    final ScanPrel newScanPrel = new ScanPrel(scan.getCluster(), filter.getTraitSet(),
        newGroupsScan, scan.getRowType(), scan.getTable());

    if (folioFilterBuilder.isAllExpressionsConverted()) {
      /*
       * Since we could convert the entire filter condition expression into an
       * Mongo filter, we can eliminate the filter operator altogether.
       */
      call.transformTo(newScanPrel);
    } else {
      call.transformTo(filter.copy(filter.getTraitSet(),
          ImmutableList.of((RelNode) newScanPrel)));
    }

  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final ScanPrel scan = (ScanPrel) call.rel(1);
    if (scan.getGroupScan() instanceof FolioGroupScan) {
      return super.matches(call);
    }
    return false;
  }

}
