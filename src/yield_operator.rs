use std::any::Any;
use std::sync::Arc;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use arrow::datatypes::Schema;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use crate::yield_stream::YieldStream;

#[derive(Debug)]
pub struct YieldStreamExec {
    child: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
    yield_batches: usize,
}

impl YieldStreamExec {
    pub fn new(child: Arc<dyn ExecutionPlan>, yield_batches: usize) -> Self {
        let properties = child.properties().clone();
        Self { child, properties, yield_batches }
    }
}

impl DisplayAs for YieldStreamExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "YieldStreamExec yield={}", self.yield_batches)
    }
}

impl ExecutionPlan for YieldStreamExec {
    fn name(&self) -> &str {
        "YieldStreamExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        self.child.schema()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        // Use Arc::clone on children[0] rather than calling clone() directly
        Ok(Arc::new(YieldStreamExec::new(Arc::clone(&children[0]), self.yield_batches)))
    }

    fn execute(
        &self,
        partition: usize,
        task_ctx: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let child_stream = self.child.execute(partition, Arc::clone(&task_ctx))?;
        let yield_stream = YieldStream::new(child_stream);
        Ok(Box::pin(yield_stream))
    }
}