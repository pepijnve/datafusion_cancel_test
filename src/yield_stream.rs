use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::task::Poll::{Pending, Ready};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use arrow::datatypes::Schema;
use futures::{Stream, StreamExt};
use datafusion::common;
use arrow::array::RecordBatch;

/// A stream that yields batches of data, yielding control back to the executor every `YIELD_BATCHES` batches
///
/// This can be useful to allow operators that might not yield to check for cancellation
pub struct YieldStream {
    inner: SendableRecordBatchStream,
    yield_batches: usize,
    batches_processed: usize,
}

impl YieldStream {
    pub fn new(inner: SendableRecordBatchStream, yield_batches: usize) -> Self {
        Self {
            inner,
            yield_batches,
            batches_processed: 0,
        }
    }
}

// Stream<Item = Result<RecordBatch>> to poll_next_unpin
impl Stream for YieldStream {
    type Item = common::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.batches_processed >= self.yield_batches {
            self.batches_processed = 0;
            // We need to buffer the batch when we return Poll::Pending,
            // so that we can return it on the next poll.
            // Otherwise, the next poll will miss the batch and return None.
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        match self.inner.poll_next_unpin(cx) {
            Ready(Some(Ok(batch))) => {
                self.batches_processed += 1;
                Ready(Some(Ok(batch)))
            }
            Pending => {
                self.batches_processed = 0;
                Pending
            },
            other => other,
        }
    }
}

// RecordBatchStream schema()
impl RecordBatchStream for YieldStream {
    fn schema(&self) -> Arc<Schema> {
        self.inner.schema()
    }
}

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
        let yield_stream = YieldStream::new(child_stream, self.yield_batches);
        Ok(Box::pin(yield_stream))
    }
}