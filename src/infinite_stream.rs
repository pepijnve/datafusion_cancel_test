use std::any::Any;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::task::Poll::Ready;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use arrow::datatypes::SchemaRef;
use datafusion::common;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use futures::Stream;
use arrow::array::RecordBatch;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};

struct InfiniteStream {
    batch: RecordBatch,
    poll_count: usize,
}

impl RecordBatchStream for InfiniteStream {
    fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }
}

impl Stream for InfiniteStream {
    type Item = common::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_count += 1;
        if self.poll_count % 10000 == 0 {
            println!("InfiniteStream::poll_next {} times", self.poll_count);
        }
        Ready(Some(Ok(self.batch.clone())))
    }
}

#[derive(Debug)]
pub struct InfiniteExec {
    batch: RecordBatch,
    properties: PlanProperties,
}

impl InfiniteExec {
    pub fn new(batch: &RecordBatch) -> Self {
        InfiniteExec {
            batch: batch.clone(),
            properties: PlanProperties::new(
                EquivalenceProperties::new(batch.schema().clone()),
                Partitioning::Hash(vec![], 1),
                EmissionType::Incremental,
                Boundedness::Unbounded {
                    requires_infinite_memory: false,
                },
            ),
        }
    }
}

impl DisplayAs for InfiniteExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "InfiniteExec")
    }
}

impl ExecutionPlan for InfiniteExec {
    fn name(&self) -> &str {
        "InfiniteExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self.clone())
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> common::Result<SendableRecordBatchStream> {
        Ok(Box::pin(InfiniteStream {
            batch: self.batch.clone(),
            poll_count: 0,
        }))
    }
}