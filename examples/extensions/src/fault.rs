//! Test-only protocol violator. Never built by the production example helper.
use sparrow_extension_sdk::*;
use std::io::{Read, Write};
fn main() {
    let mut input = std::io::stdin().lock();
    let mut output = std::io::stdout().lock();
    let mut mode = String::new();
    let mut role = Role::Transform;
    loop {
        let mut header = [0; 4];
        if input.read_exact(&mut header).is_err() {
            return;
        }
        let size = u32::from_be_bytes(header) as usize;
        if size > MAX_FRAME {
            return;
        }
        let mut data = vec![0; size];
        input.read_exact(&mut data).unwrap();
        let request: Request = serde_json::from_slice(&data).unwrap();
        let mut sequence = request.sequence;
        let reply = match request.operation {
            Operation::Describe { role: r, .. } => {
                role = r;
                let f = Field {
                    name: "value".into(),
                    kind: Type::Int64,
                    nullable: true,
                };
                Reply::Description {
                    protocol: PROTOCOL.into(),
                    declaration: Declaration {
                        role,
                        input: if role == Role::Source {
                            vec![]
                        } else {
                            vec![f.clone()]
                        },
                        output: if role == Role::Sink { vec![] } else { vec![f] },
                        limits: Limits {
                            max_rows: 16,
                            max_frame_bytes: 16384,
                        },
                        permissions: vec![],
                        watermarks: role == Role::Source,
                    },
                }
            }
            Operation::Open { config } => {
                mode = config["mode"].as_str().unwrap().into();
                Reply::Opened
            }
            Operation::Close => Reply::Closed,
            Operation::Accepted { .. } => Reply::Accepted,
            Operation::Flush => {
                if mode == "flush_fail" {
                    Reply::Failure { code: Code::Io }
                } else {
                    Reply::Flushed
                }
            }
            _ => {
                match mode.as_str() {
                    "hang" => loop {
                        std::thread::sleep(std::time::Duration::from_secs(1));
                    },
                    "crash" => std::process::exit(13),
                    "oom" => {
                        let mut v = Vec::new();
                        loop {
                            v.push(vec![1u8; 1024 * 1024]);
                            std::hint::black_box(&v);
                        }
                    }
                    "oversize" => {
                        output.write_all(&u32::MAX.to_be_bytes()).unwrap();
                        output.flush().unwrap();
                        return;
                    }
                    "sequence" => sequence += 1,
                    _ => {}
                }
                if mode == "environment" {
                    let clean = std::env::vars_os().count() == 0
                        && std::env::current_dir().unwrap() == std::path::Path::new("/")
                        && std::fs::read_dir("/proc/self/fd").unwrap().count() <= 4;
                    Reply::Rows {
                        rows: vec![vec![Value::Int(i64::from(clean).to_string())]],
                    }
                } else if role == Role::Sink {
                    Reply::Accepted
                } else {
                    let rows = if mode == "schema" {
                        vec![vec![Value::Text("wrong".into())]]
                    } else if mode == "expansion" {
                        (0..17).map(|_| vec![Value::Int("1".into())]).collect()
                    } else {
                        vec![vec![Value::Int("1".into())]]
                    };
                    if role == Role::Source {
                        Reply::Data {
                            rows,
                            watermark: Some(if sequence <= 3 { 10 } else { 9 }),
                        }
                    } else {
                        Reply::Rows { rows }
                    }
                }
            }
        };
        let closed = matches!(reply, Reply::Closed);
        let data = serde_json::to_vec(&Response { sequence, reply }).unwrap();
        output
            .write_all(&(data.len() as u32).to_be_bytes())
            .unwrap();
        output.write_all(&data).unwrap();
        output.flush().unwrap();
        if closed {
            return;
        }
    }
}
