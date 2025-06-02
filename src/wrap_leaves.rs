use std::sync::Arc;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::config::ConfigOptions;
use datafusion::common;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::physical_plan::execution_plan::EmissionType;
use crate::yield_stream::YieldStreamExec;

#[derive(Debug)]
pub struct WrapLeaves {}

fn wrap_leaves(
    plan: Arc<dyn ExecutionPlan>,
) -> common::Result<Transformed<Arc<dyn ExecutionPlan>>> {
    if plan.children().is_empty() {
        // Leaf node: wrap it in `YieldStreamExec` and stop recursing
        Ok(Transformed::new(
            Arc::new(YieldStreamExec::new(plan, 64)),
            true,
            TreeNodeRecursion::Jump,
        ))
    } else {
        Ok(Transformed::no(plan))
    }
}

fn wrap_leaves_of_pipeline_breakers(
    plan: Arc<dyn ExecutionPlan>,
) -> common::Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let is_pipeline_breaker = plan.properties().emission_type == EmissionType::Final;
    if is_pipeline_breaker {
        let mut transformed = plan.transform_down(wrap_leaves)?;
        transformed.tnr = TreeNodeRecursion::Jump;
        Ok(transformed)
    } else {
        Ok(Transformed::no(plan))
    }
}

impl PhysicalOptimizerRule for WrapLeaves {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _: &ConfigOptions,
    ) -> common::Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(wrap_leaves_of_pipeline_breakers).data()
    }

    fn name(&self) -> &str {
        "wrap"
    }

    fn schema_check(&self) -> bool {
        true
    }
}