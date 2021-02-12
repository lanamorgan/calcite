package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.ViewExpanders;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;

import org.apache.calcite.tools.RelBuilderFactory;

/**
 * Planner rule that converts a
 * {@link org.apache.calcite.rel.logical.LogicalTableScan} to the result
 * of calling {@link RelOptTable#toRel}.
 *
 * {@code org.apache.calcite.rel.core.RelFactories.TableScanFactoryImpl}
 * has called {@link RelOptTable#toRel(RelOptTable.ToRelContext)}.
 */

public abstract class AnyExprMergeRule <C extends AnyExprMergeRule.Config>
    extends RelRule<C>
    implements TransformationRule {
  //~ Static fields/initializers ---------------------------------------------

  //~ Constructors -----------------------------------------------------------

  /** Creates a TableScanRule. */
  protected AnyExprMergeRule(C config) {
    super(config);
  }


  //~ Methods ----------------------------------------------------------------

  protected void perform(RelOptRuleCall call, LogicalProject project) {
    // if not nested, abort
//    List<RexNode> projects = project.getProjects();
//    int diffIndex = -1;
////    for (int i = 0; i < projects.len() && diffIndex < 0; i++){
//      if (projects[i] instanceof RexAny){
//        RexAny p = (RexAny) projects[i];
//        if (p.hasDiffChild())
//          diffIndex = i;
//      }
//    }
//    if (diffIndex < 0)
//      return;

  }

  public static class ProjectAnyMergeRule
      extends AnyExprMergeRule<AnyExprMergeRule.Config> {

    protected ProjectAnyMergeRule(Config config) {super(config);}

    @Override public void onMatch(RelOptRuleCall call) {
      LogicalProject project = call.rel(0);
      perform(call, project);
    }

    public interface Config extends AnyExprMergeRule.Config {
      Config DEFAULT = EMPTY
          .withOperandSupplier(b0 ->
              b0.operand(LogicalProject.class).predicate(LogicalProject::hasDiffNode)
                  .anyInputs())
          .as(ProjectAnyMergeRule.Config.class);

      @Override
      default ProjectAnyMergeRule toRule() {
        return new ProjectAnyMergeRule(this);
      }
    }
  }
}
