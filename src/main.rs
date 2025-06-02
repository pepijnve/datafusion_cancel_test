use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use datafusion::common::ScalarValue::Int64;
use datafusion::config::ConfigOptions;
use datafusion::functions_aggregate::sum;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::aggregate::AggregateExprBuilder;
use datafusion::physical_expr::expressions::{binary, col, lit};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::union::InterleaveExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_test::infinite_stream::InfiniteExec;
use datafusion_test::wrap_leaves::{WrapChildren, WrapLeaves};
use futures::StreamExt;
use std::error::Error;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let session_context = SessionContext::new();

    let schema = Arc::new(Schema::new(Fields::try_from(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )])?));

    let mut column_builder = Int64Array::builder(8192);
    for v in 0..8192 {
        column_builder.append_value(v as i64);
    }
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(column_builder.finish())])?;

    let mut branches: Vec<Arc<dyn ExecutionPlan>> = vec![];
    for i in 0..10 {
        let inf1 = Arc::new(InfiniteExec::new(&batch));
        let filter1 = Arc::new(FilterExec::try_new(
            binary(
                col("value", &schema)?,
                Operator::Gt,
                lit(Int64(Some(8192 - (i + 1) * 81))),
                &schema,
            )?,
            inf1,
        )?);
        let coalesce1 = Arc::new(CoalesceBatchesExec::new(
            filter1,
            session_context.copied_config().batch_size()
        ));

        branches.push(coalesce1);
    }

    let interleave = Arc::new(InterleaveExec::try_new(branches)?);
    let aggr_total = Arc::new(AggregateExec::try_new(
        AggregateMode::Single,
        PhysicalGroupBy::new(vec![], vec![], vec![]),
        vec![Arc::new(
            AggregateExprBuilder::new(
                sum::sum_udaf(),
                vec![col("value", &interleave.schema())?],
            )
                .schema(interleave.schema().clone())
                .alias("total")
                .build()?,
        )],
        vec![None],
        interleave.clone(),
        interleave.schema(),
    )?);
    
    let plan = WrapLeaves {}.optimize(aggr_total, &ConfigOptions::default())?;
    // let plan = WrapChildren {}.optimize(aggr_total, &ConfigOptions::default())?;

    let displayable_execution_plan = physical_plan::displayable(&*plan);

    println!("Physical plan");
    println!("=============");
    println!("{}", displayable_execution_plan.indent(false));

    let mut stream = physical_plan::execute_stream(plan, session_context.task_ctx())?;

    println!("Running query");
    let next = tokio::select! {
        res = stream.next() => res,
        _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {
            println!("Cancellation received!");
            None
        },
    };

    match next {
        None => {
            println!("No result");
        }
        Some(Ok(batch)) => {
            println!("Batch of size {}", batch.num_rows());
        }
        Some(Err(_)) => {
            println!("Error");
        }
    }

    println!("Dropping stream");
    drop(stream);

    Ok(())
}

