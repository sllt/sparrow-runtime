//! Build without Sparrow host crates; one executable offers role-specific
//! declarations. Production packages approve each role separately.
use serde::Deserialize;
use sparrow_extension_sdk::*;
use std::{
    fs::{File, OpenOptions},
    io::Write,
};

#[derive(Default)]
struct Example {
    role: Option<Role>,
    next: i64,
    remaining: u64,
    factor: i64,
    copies: usize,
    file: Option<File>,
}
fn field(nullable: bool) -> Field {
    Field {
        name: "value".into(),
        kind: Type::Int64,
        nullable,
    }
}
fn declaration(role: Role) -> Declaration {
    Declaration {
        role,
        input: if role == Role::Source {
            vec![]
        } else {
            vec![field(true)]
        },
        output: if role == Role::Sink {
            vec![]
        } else {
            vec![field(role == Role::Transform)]
        },
        limits: Limits {
            max_rows: 16,
            max_frame_bytes: 16384,
        },
        permissions: if role == Role::Sink {
            vec![Permission::Filesystem]
        } else {
            vec![]
        },
        watermarks: false,
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Counter {
    start: i64,
    count: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Multiply {
    factor: i64,
    copies: usize,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Output {
    path: String,
}
impl Plugin for Example {
    fn declaration(&self) -> Declaration {
        declaration(self.role.unwrap())
    }
    fn open(&mut self, config: &serde_json::Value) -> Result<()> {
        match self.role.unwrap() {
            Role::Source => {
                let c: Counter =
                    serde_json::from_value(config.clone()).map_err(|_| Code::InvalidConfig)?;
                if c.count > 1_000_000
                    || (c.count > 0 && c.start.checked_add((c.count - 1) as i64).is_none())
                {
                    return Err(Code::Limit);
                }
                self.next = c.start;
                self.remaining = c.count;
            }
            Role::Transform => {
                let c: Multiply =
                    serde_json::from_value(config.clone()).map_err(|_| Code::InvalidConfig)?;
                if !(1..=16).contains(&c.copies) {
                    return Err(Code::Limit);
                }
                self.factor = c.factor;
                self.copies = c.copies;
            }
            Role::Sink => {
                let c: Output =
                    serde_json::from_value(config.clone()).map_err(|_| Code::InvalidConfig)?;
                if c.path.len() > 1024 || !std::path::Path::new(&c.path).is_absolute() {
                    return Err(Code::InvalidConfig);
                }
                use std::os::unix::fs::OpenOptionsExt;
                // New files only; no truncation of user data, no final symlink.
                // Ancestor directories are administrator-controlled, not a sandbox.
                self.file = Some(
                    OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .mode(0o600)
                        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
                        .open(&c.path)
                        .map_err(|_| Code::Io)?,
                );
            }
        }
        Ok(())
    }
    fn poll(&mut self, max_rows: usize) -> Result<Poll> {
        if self.remaining == 0 {
            return Ok(Poll::End);
        }
        let count = self.remaining.min(max_rows as u64);
        let mut rows = Vec::with_capacity(count as usize);
        for _ in 0..count {
            rows.push(vec![Value::Int(self.next.to_string())]);
            self.remaining -= 1;
            if self.remaining > 0 {
                self.next = self.next.checked_add(1).ok_or(Code::Limit)?;
            }
        }
        Ok(Poll::Data {
            rows,
            watermark: None,
        })
    }
    fn transform(&self, row: &Row) -> Result<Vec<Row>> {
        let value = match &row[0] {
            Value::Null => Value::Null,
            Value::Int(v) => Value::Int(
                v.parse::<i64>()
                    .map_err(|_| Code::Protocol)?
                    .checked_mul(self.factor)
                    .ok_or(Code::Limit)?
                    .to_string(),
            ),
            _ => return Err(Code::Protocol),
        };
        Ok((0..self.copies).map(|_| vec![value.clone()]).collect())
    }
    fn push(&mut self, rows: &[Row]) -> Result<()> {
        let file = self.file.as_mut().ok_or(Code::Io)?;
        for row in rows {
            let value = match &row[0] {
                Value::Null => serde_json::Value::Null,
                Value::Int(v) => serde_json::json!(v.parse::<i64>().map_err(|_| Code::Protocol)?),
                _ => return Err(Code::Protocol),
            };
            serde_json::to_writer(&mut *file, &serde_json::json!({"value":value}))
                .map_err(|_| Code::Io)?;
            file.write_all(b"\n").map_err(|_| Code::Io)?;
        }
        Ok(())
    }
    fn flush(&mut self) -> Result<()> {
        if let Some(file) = &self.file {
            file.sync_data().map_err(|_| Code::Io)?;
        }
        Ok(())
    }
}
fn main() {
    if serve(|role| {
        Ok(Box::new(Example {
            role: Some(role),
            ..Default::default()
        }))
    })
    .is_err()
    {
        std::process::exit(1);
    }
}
