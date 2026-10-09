//! CSV File source: header handling, BOM, multiline framing and exact resume.
use super::*;
use sparrow_formats::{CsvOptions, CsvRole};
use sparrow_model::{DataType, Field, FieldId, Scalar, SchemaId};

fn schema() -> Schema {
    Schema::new(
        SchemaId::new(1),
        vec![
            Field::new(FieldId::new(1), "device_id", DataType::Utf8, false),
            Field::new(FieldId::new(2), "v", DataType::Int64, false),
        ],
    )
    .unwrap()
}

fn tmp(name: &str) -> PathBuf {
    crate::policy::ensure_default_data_root().join(format!(
        "sparrow-file-csv-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn config(path: &Path, options: CsvOptions, contract: FileContract) -> FileReplayConfig {
    let mut cfg = FileReplayConfig::new(path, schema());
    cfg.contract = contract;
    cfg.format = PayloadFormat::csv(options.compile(CsvRole::Decode).unwrap());
    cfg
}

fn row(id: &str, v: i64) -> Vec<Scalar> {
    vec![Scalar::utf8(id), Scalar::Int64(v)]
}

#[derive(Debug, PartialEq)]
enum Seen {
    Row(Vec<Scalar>),
    Bad,
}

/// Every poll result until EOF, with the cut taken after each poll.
fn drain(src: &mut FileReplaySource) -> Vec<(Seen, SourcePosition)> {
    let mut out = Vec::new();
    loop {
        match src.poll_decoded().unwrap() {
            FilePoll::Row(row) => out.push((Seen::Row(row.values), src.position())),
            FilePoll::DecodeError => out.push((Seen::Bad, src.position())),
            FilePoll::Pending => continue,
            FilePoll::Eof => return out,
        }
    }
}

/// Resuming from every cut (including the start and the header boundary)
/// yields exactly the remaining suffix: no row lost, none repeated.
fn assert_exact_resume(cfg: &FileReplayConfig, expected: &[Seen]) {
    let mut full = FileReplaySource::open(cfg).unwrap();
    let start = full.position();
    let polls = drain(&mut full);
    let seen: Vec<&Seen> = polls.iter().map(|(seen, _)| seen).collect();
    assert_eq!(seen, expected.iter().collect::<Vec<_>>());
    let mut cuts = vec![(0, start)];
    cuts.extend(
        polls
            .iter()
            .enumerate()
            .map(|(i, (_, cut))| (i + 1, cut.clone())),
    );
    for (done, cut) in cuts {
        let mut resumed = FileReplaySource::open(cfg).unwrap();
        resumed.seek(&cut).unwrap();
        let rest: Vec<Seen> = drain(&mut resumed).into_iter().map(|(s, _)| s).collect();
        assert_eq!(rest, expected[done..], "resume after {done} polls");
    }
}

#[test]
fn review_csv_file_bare_cr_is_not_silently_removed_at_eof_or_crlf() {
    for tail in [b"x\r".as_slice(), b"x\r\r\n", b"x\r\n"] {
        let path = tmp("bare-cr");
        let mut content = b"v\n".to_vec();
        content.extend_from_slice(tail);
        std::fs::write(&path, content).unwrap();
        let mut cfg = config(&path, CsvOptions::default(), FileContract::Sealed);
        cfg.schema = Schema::new(1, vec![Field::new(1, "v", DataType::Utf8, false)]).unwrap();
        let mut source = FileReplaySource::open(&cfg).unwrap();
        let got = drain(&mut source);
        assert_eq!(got.len(), 1);
        assert_eq!(
            got[0].0,
            if tail == b"x\r\n" {
                Seen::Row(vec![Scalar::utf8("x")])
            } else {
                Seen::Bad
            }
        );
        drop(source);
        std::fs::remove_file(path).unwrap();
    }
}

#[test]
fn csv_header_bom_crlf_blank_lines_and_exact_resume() {
    let path = tmp("basic");
    fs::write(
        &path,
        b"\xEF\xBB\xBF\r\ndevice_id,v\r\na,1\r\n\r\n\"b,\"\"x\"\"\",2\nbad,notint\nc,3",
    )
    .unwrap();
    let cfg = config(&path, CsvOptions::default(), FileContract::Sealed);
    let expected = [
        Seen::Row(row("a", 1)),
        Seen::Row(row("b,\"x\"", 2)),
        Seen::Bad,
        Seen::Row(row("c", 3)),
    ];
    assert_exact_resume(&cfg, &expected);
    // The header is consumed, never a row, and a type error is classified.
    let diag = IoDiagnostics::new();
    let mut src = FileReplaySource::open(&cfg).unwrap();
    src.set_diagnostics(diag.clone());
    assert_eq!(drain(&mut src).len(), 4);
    let snap = diag.snapshot();
    assert_eq!((snap.csv_type_errors, snap.csv_malformed), (1, 0));
    // fail_on_decode turns the bad row into a source error.
    let mut strict = cfg.clone();
    strict.fail_on_decode = true;
    let mut src = FileReplaySource::open(&strict).unwrap();
    assert!(matches!(src.poll_decoded().unwrap(), FilePoll::Row(_)));
    assert!(matches!(src.poll_decoded().unwrap(), FilePoll::Row(_)));
    assert!(matches!(src.poll_decoded().unwrap(), FilePoll::DecodeError));
    fs::remove_file(path).unwrap();
}

#[test]
fn csv_header_maps_by_name_and_header_errors_are_fatal() {
    let path = tmp("names");
    fs::write(&path, "v,extra,device_id\n1,z,a\n2,,b\n").unwrap();
    let cfg = config(&path, CsvOptions::default(), FileContract::Sealed);
    assert_exact_resume(&cfg, &[Seen::Row(row("a", 1)), Seen::Row(row("b", 2))]);
    let strict = config(
        &path,
        CsvOptions {
            extra_columns: sparrow_formats::csv::ExtraColumns::Error,
            ..Default::default()
        },
        FileContract::Sealed,
    );
    let cases: [(&str, Vec<u8>, &FileReplayConfig); 4] = [
        ("missing", b"device_id\na\n".to_vec(), &cfg),
        ("extra", b"v,extra,device_id\n1,z,a\n".to_vec(), &strict),
        ("broken", b"device_id,\"v\n1,a\n".to_vec(), &cfg),
        (
            "oversize",
            {
                let mut header = vec![b'h'; 70 * 1024];
                header.extend_from_slice(b"\na,1\n");
                header
            },
            &cfg,
        ),
    ];
    for (name, body, cfg) in cases {
        fs::write(&path, body).unwrap();
        let diag = IoDiagnostics::new();
        let mut src = FileReplaySource::open(cfg).unwrap();
        src.set_diagnostics(diag.clone());
        let error = loop {
            match src.poll_decoded() {
                Ok(FilePoll::Pending) => continue,
                Ok(other) => panic!("{name}: header error must be fatal, got {other:?}"),
                Err(error) => break error,
            }
        };
        assert_eq!(error.code, ErrorCode::InvalidSchema, "{name}: {error}");
        assert_eq!(diag.snapshot().csv_header_errors, 1, "{name}");
    }
    fs::remove_file(path).unwrap();
}

#[test]
fn csv_multiline_quoted_newlines_resume_exactly_and_refuse_mid_record_cuts() {
    let path = tmp("multiline");
    let body = "device_id,v\n\"line1\nline2\",1\n\"x\r\n\",2\ny,3\n";
    fs::write(&path, body).unwrap();
    let options = CsvOptions {
        multiline: true,
        ..Default::default()
    };
    let cfg = config(&path, options, FileContract::Sealed);
    let expected = [
        Seen::Row(row("line1\nline2", 1)),
        Seen::Row(row("x\r\n", 2)),
        Seen::Row(row("y", 3)),
    ];
    assert_exact_resume(&cfg, &expected);
    // A cut right after the quoted line break: the previous byte is `\n`,
    // but the offset is inside a record.
    let mut src = FileReplaySource::open(&cfg).unwrap();
    let mut inside = src.position();
    inside.offset_bytes = "device_id,v\n\"line1\n".len() as u64;
    inside.record_index = 2;
    let error = src.seek(&inside).unwrap_err();
    assert_eq!(error.code, ErrorCode::InvalidArgument, "{error}");
    // Without multiline a quoted line break is refused as malformed, and the
    // rows after it still decode.
    let single = config(&path, CsvOptions::default(), FileContract::Sealed);
    let diag = IoDiagnostics::new();
    let mut src = FileReplaySource::open(&single).unwrap();
    src.set_diagnostics(diag.clone());
    let seen: Vec<Seen> = drain(&mut src).into_iter().map(|(s, _)| s).collect();
    assert_eq!(
        seen,
        [
            Seen::Bad,
            Seen::Bad,
            Seen::Bad,
            Seen::Bad,
            Seen::Row(row("y", 3))
        ]
    );
    assert_eq!(diag.snapshot().csv_malformed, 4);
    fs::remove_file(path).unwrap();
}

#[test]
fn csv_headerless_columns_oversize_and_bom_only_at_start() {
    let path = tmp("headerless");
    let big = "x".repeat(70 * 1024);
    fs::write(&path, format!("\u{feff}1,a\n{big},1\n2,b\n\u{feff}3,c\n")).unwrap();
    let options = CsvOptions {
        header: false,
        columns: Some(vec!["v".into(), "device_id".into()]),
        ..Default::default()
    };
    let cfg = config(&path, options, FileContract::Immutable);
    let expected = [
        Seen::Row(row("a", 1)),
        Seen::Bad,
        Seen::Row(row("b", 2)),
        Seen::Bad,
    ];
    assert_exact_resume(&cfg, &expected);
    let diag = IoDiagnostics::new();
    let mut src = FileReplaySource::open(&cfg).unwrap();
    src.set_diagnostics(diag.clone());
    drain(&mut src);
    let snap = diag.snapshot();
    assert_eq!((snap.csv_oversize, snap.csv_malformed), (1, 1));
    fs::remove_file(path).unwrap();
}

#[test]
fn csv_append_only_header_split_across_appends() {
    use std::io::Write;
    let path = tmp("append");
    fs::write(&path, b"devi").unwrap();
    let cfg = config(&path, CsvOptions::default(), FileContract::AppendOnly);
    let mut src = FileReplaySource::open(&cfg).unwrap();
    assert!(matches!(src.poll_decoded().unwrap(), FilePoll::Eof));
    let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
    file.write_all(b"ce_id,v\na,").unwrap();
    assert!(matches!(src.poll_decoded().unwrap(), FilePoll::Eof));
    // The header is consumed: the cut sits after it, before the partial row.
    let cut = src.checkpoint_position().unwrap();
    assert_eq!(cut.offset_bytes, "device_id,v\n".len() as u64);
    file.write_all(b"1\nb,2\n").unwrap();
    let rows: Vec<Seen> = drain(&mut src).into_iter().map(|(s, _)| s).collect();
    assert_eq!(rows, [Seen::Row(row("a", 1)), Seen::Row(row("b", 2))]);
    let mut resumed = FileReplaySource::open(&cfg).unwrap();
    resumed.seek(&cut).unwrap();
    let rows: Vec<Seen> = drain(&mut resumed).into_iter().map(|(s, _)| s).collect();
    assert_eq!(rows, [Seen::Row(row("a", 1)), Seen::Row(row("b", 2))]);
    fs::remove_file(path).unwrap();
}

#[test]
fn csv_config_checks_schema_layout() {
    let path = tmp("schema");
    fs::write(&path, b"").unwrap();
    let single = Schema::new(
        SchemaId::new(1),
        vec![Field::new(FieldId::new(1), "v", DataType::Int64, true)],
    )
    .unwrap();
    let mut cfg = FileReplayConfig::new(&path, single);
    cfg.format = PayloadFormat::csv(CsvOptions::default().compile(CsvRole::Decode).unwrap());
    assert_eq!(cfg.validate().unwrap_err().code, ErrorCode::InvalidArgument);
    fs::remove_file(path).unwrap();
}

#[test]
fn csv_checkpoint_identity_binds_format_and_options() {
    for contract in [FileContract::Sealed, FileContract::AppendOnly] {
        let path = tmp("identity");
        fs::write(&path, b"device_id,v\na,1\nb,2\n").unwrap();
        let cfg = config(&path, CsvOptions::default(), contract);
        let mut src = FileReplaySource::open(&cfg).unwrap();
        assert!(matches!(src.poll_decoded().unwrap(), FilePoll::Row(_)));
        let cut = src.checkpoint_position().unwrap();
        // Same format and options: the cut restores.
        let mut same = FileReplaySource::open(&cfg).unwrap();
        same.seek(&cut).unwrap();
        assert!(matches!(same.poll_decoded().unwrap(), FilePoll::Row(_)));
        // Explicit defaults are the same effective options: also restores.
        let explicit = config(
            &path,
            CsvOptions {
                delimiter: ",".into(),
                max_record_bytes: Some(65536),
                max_fields: Some(256),
                ..Default::default()
            },
            contract,
        );
        let mut same = FileReplaySource::open(&explicit).unwrap();
        same.seek(&cut).unwrap();
        assert!(matches!(same.poll_decoded().unwrap(), FilePoll::Row(_)));
        let mut json = FileReplayConfig::new(&path, schema());
        json.contract = contract;
        let others = [
            json,
            config(
                &path,
                CsvOptions {
                    trim: true,
                    ..Default::default()
                },
                contract,
            ),
            config(
                &path,
                CsvOptions {
                    null_value: "NULL".into(),
                    ..Default::default()
                },
                contract,
            ),
            config(
                &path,
                CsvOptions {
                    max_fields: Some(16),
                    ..Default::default()
                },
                contract,
            ),
            config(
                &path,
                CsvOptions {
                    delimiter: ";".into(),
                    ..Default::default()
                },
                contract,
            ),
        ];
        for other in &others {
            let mut src = FileReplaySource::open(other).unwrap();
            let refused = src.seek(&cut).unwrap_err();
            assert_eq!(refused.code, ErrorCode::UnsupportedRestore, "{contract:?}");
            assert!(
                refused.message.contains("CSV options"),
                "{}",
                refused.message
            );
        }
        // An NDJSON cut is not adopted by a CSV reader either; JSON identities
        // are unchanged by this binding.
        let json = &others[0];
        let src = FileReplaySource::open(json).unwrap();
        let json_cut = src.checkpoint_position().unwrap();
        assert_eq!(
            FileReplaySource::open(&cfg)
                .unwrap()
                .seek(&json_cut)
                .unwrap_err()
                .code,
            ErrorCode::UnsupportedRestore
        );
        let mut probe = File::open(&path).unwrap();
        let content = content_fingerprint(&mut probe, json_cut.identity.size).unwrap();
        assert_eq!(json_cut.identity.fingerprint, content);
        fs::remove_file(path).unwrap();
    }
}

#[test]
fn protobuf_is_refused_by_file_sources() {
    let path = tmp("protobuf");
    std::fs::write(&path, b"").unwrap();
    let mut cfg = FileReplayConfig::new(&path, schema());
    cfg.format = crate::protobuf_test_support::format(CsvRole::Decode, |_| {});
    assert_eq!(cfg.validate().unwrap_err().code, ErrorCode::FeatureUnavailable);
    let err = FileReplaySource::open(&cfg).err().expect("open must refuse");
    assert_eq!(err.code, ErrorCode::FeatureUnavailable);
    std::fs::remove_file(&path).unwrap();
}
