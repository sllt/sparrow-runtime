//! SQL text and GraphSpec bind to the same IR and produce identical capture.
//!
//! ```text
//! cargo run -p sparrow-testkit --example m1_sql_graph_equiv
//! ```

use std::path::PathBuf;

use sparrow_model::{PipelineId, RevisionId};
use sparrow_plan::{bind_graph, physicalize, Catalog, GraphSpec, PlanOptions};
use sparrow_runtime::{JobRequest, Kernel, KernelOptions, SharedCapture};
use sparrow_sql::bind_sql;
use sparrow_testkit::{sensor_fixture, sensor_schema};

fn main() {
    if let Err(err) = run() {
        eprintln!("m1_sql_graph_equiv failed: {err}");
        std::process::exit(1);
    }
}

fn graph_path() -> PathBuf {
    let candidates = [
        PathBuf::from("tests/fixtures/graph/sensor_filter_project.json"),
        PathBuf::from("../tests/fixtures/graph/sensor_filter_project.json"),
        PathBuf::from("../../tests/fixtures/graph/sensor_filter_project.json"),
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/fixtures/graph/sensor_filter_project.json"),
    ];
    for c in &candidates {
        if c.is_file() {
            return c.clone();
        }
    }
    candidates[3].clone()
}

fn run() -> sparrow_model::Result<()> {
    let mut catalog = Catalog::new();
    catalog.insert("sensor_readings", sensor_schema());

    let sql = "SELECT device_id, temperature, ts FROM sensor_readings WHERE temperature > 25";
    let sql_bound = bind_sql(sql, &catalog, PipelineId::new(7), RevisionId::new(1))?;

    let json = std::fs::read_to_string(graph_path()).map_err(|e| {
        sparrow_model::SparrowError::new(
            sparrow_model::ErrorCode::InvalidArgument,
            format!("read GraphSpec: {e}"),
        )
    })?;
    let spec = GraphSpec::from_json(&json)?;
    let graph_bound = bind_graph(&spec, &catalog)?;

    // Graph projects payload_temp as well; trim SQL vs graph to shared columns
    // by running Graph as specified and SQL as specified, then compare
    // device_id + temperature on both captures after aligning.
    let kernel = Kernel::new(KernelOptions::default())?;
    let rows: Vec<_> = sensor_fixture().into_iter().map(|r| r.to_row()).collect();

    let sql_cap = SharedCapture::new();
    kernel.run(JobRequest::new(
        physicalize(&sql_bound, &PlanOptions { fuse: true }),
        rows.clone(),
        sql_cap.clone(),
    ))?;

    let graph_cap = SharedCapture::new();
    kernel.run(JobRequest::new(
        physicalize(&graph_bound, &PlanOptions { fuse: true }),
        rows,
        graph_cap.clone(),
    ))?;

    println!("sql captured:");
    for (i, row) in sql_cap.rows_as_debug().iter().enumerate() {
        println!("  {i}: {}", row.join(" | "));
    }
    println!("graph captured:");
    for (i, row) in graph_cap.rows_as_debug().iter().enumerate() {
        println!("  {i}: {}", row.join(" | "));
    }

    if sql_cap.rows() != graph_cap.rows() {
        return Err(sparrow_model::SparrowError::new(
            sparrow_model::ErrorCode::Internal,
            format!(
                "SQL/Graph mismatch: {:?} vs {:?}",
                sql_cap.rows_as_debug(),
                graph_cap.rows_as_debug()
            ),
        ));
    }
    if kernel.live_tasks() != 0 {
        return Err(sparrow_model::SparrowError::new(
            sparrow_model::ErrorCode::Internal,
            "orphan tasks",
        ));
    }
    println!("m1_sql_graph_equiv: ok ({} rows)", sql_cap.row_count());
    Ok(())
}
