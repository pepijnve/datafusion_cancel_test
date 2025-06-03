use crate::maybe_poll;
use crate::poll_budget::PollBudget;
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use datafusion::common;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use futures::{Stream, StreamExt};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// A convenience stream that combines a stream with a poll budget.
pub struct YieldStream {
    inner: SendableRecordBatchStream,
    poll_budget: PollBudget,
}

impl YieldStream {
    pub fn new(inner: SendableRecordBatchStream) -> Self {
        Self {
            inner,
            poll_budget: PollBudget::default(),
        }
    }
}

impl Stream for YieldStream {
    type Item = common::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        maybe_poll!(self.inner, .poll_next_unpin(cx), self.poll_budget)
    }
}

impl RecordBatchStream for YieldStream {
    fn schema(&self) -> Arc<Schema> {
        self.inner.schema()
    }
}