use std::task::Poll::{Pending, Ready};
use std::task::{Context, Poll};

/// A support type that can be used in Stream implementation to avoid blocking the tokio executor
/// for an extended period of time when a polled child stream is always ready
pub struct PollBudget {
    budget: usize,
    consumed: usize,
}

impl Default for PollBudget {
    fn default() -> Self {
        Self::new(64)
    }
}

impl PollBudget {
    pub fn new(budget: usize) -> Self {
        Self {
            budget,
            consumed: 0,
        }
    }
    
    pub fn reset_budget(&mut self) {
        self.consumed = 0;
    }

    pub fn consume_budget(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        self.consumed += 1;
        if self.consumed >= self.budget {
            self.consumed = 0;
            cx.waker().wake_by_ref();
            Pending
        } else {
            Ready(())
        }
    }
}

#[macro_export]
macro_rules! maybe_poll {
    // Munch TTs until we find the final function call assuming it's a call to poll_next like
    // function. Expand to the argument list.
    (@poll_args ( $( $args:expr ),* ) ) => { $($args),* };
    (@poll_args $head:tt $($tail:tt)+) => { maybe_poll!(@poll_args $($tail)+) };
    
    ($budget:expr, $($poll_call:tt)*) => {
        {
            match $budget.consume_budget(maybe_poll!(@poll_args $($poll_call)*)) {
                std::task::Poll::Ready(_) => {
                    let poll = $($poll_call)*;
                    if poll.is_pending() {
                        $budget.reset_budget();
                    }
                    poll
                }
                std::task::Poll::Pending => std::task::Poll::Pending
            }
        }
    };
}