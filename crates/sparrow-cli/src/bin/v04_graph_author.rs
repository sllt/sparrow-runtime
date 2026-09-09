//! Minimal offline Graph authoring: template / validate / explain.
//! Not a Web Designer product.

use sparrow_plan::{et_tumble_template, explain_graph, Catalog, GraphSpec};

fn usage() -> ! {
    eprintln!("usage: v04_graph_author template|validate|explain [FILE|-]");
    std::process::exit(2);
}

fn main() {
    if let Err(e) = run() {
        eprintln!("v04_graph_author failed: {e}");
        std::process::exit(1);
    }
}

fn run() -> sparrow_model::Result<()> {
    let mut args = std::env::args().skip(1);
    let cmd = args.next().unwrap_or_else(|| usage());
    match cmd.as_str() {
        "template" => {
            print!("{}", et_tumble_template());
            Ok(())
        }
        "validate" | "explain" => {
            let path = args.next().unwrap_or_else(|| "-".into());
            let text = if path == "-" {
                use std::io::Read;
                let mut buf = String::new();
                std::io::stdin().read_to_string(&mut buf).map_err(|e| {
                    sparrow_model::SparrowError::new(
                        sparrow_model::ErrorCode::InvalidArgument,
                        format!("stdin: {e}"),
                    )
                })?;
                buf
            } else {
                std::fs::read_to_string(&path).map_err(|e| {
                    sparrow_model::SparrowError::new(
                        sparrow_model::ErrorCode::InvalidArgument,
                        format!("read {path}: {e}"),
                    )
                })?
            };
            let spec = GraphSpec::from_json(&text)?;
            if cmd == "validate" {
                let bound = sparrow_plan::validate_graph(&spec, &Catalog::new())?;
                println!("accepted=true nodes={}", bound.nodes.len());
                println!("v04_graph_author: validate ok");
                return Ok(());
            }
            let report = explain_graph(&spec, &Catalog::new())?;
            println!("accepted={}", report.accepted);
            println!("physical={}", report.physical.join(" | "));
            println!("fusion={}", report.fusion);
            println!("time={}", report.time);
            println!("state={}", report.state);
            println!("guarantee={}", report.guarantee);
            println!("delivery={}", report.delivery);
            println!("recovery={}", report.recovery);
            println!("honesty={}", report.honesty);
            println!("v04_graph_author: explain ok");
            Ok(())
        }
        _ => usage(),
    }
}
