//! Standalone synchronous plugin SDK. The host supplies process/resource
//! isolation. This is not a full OS sandbox or durable ACK/checkpoint protocol.
pub mod protocol;
pub use protocol::*;
use std::io::{Read, Write};
pub type Result<T> = std::result::Result<T, Code>;
pub enum Poll {
    Data {
        rows: Vec<Row>,
        watermark: Option<i64>,
    },
    Idle {
        retry_after_ms: u64,
    },
    End,
}
pub trait Plugin {
    fn declaration(&self) -> Declaration;
    fn open(&mut self, _config: &serde_json::Value) -> Result<()> {
        Ok(())
    }
    fn poll(&mut self, _max_rows: usize) -> Result<Poll> {
        Err(Code::Unsupported)
    }
    /// Volatile admission only. MUST NOT be interpreted as a committed checkpoint.
    fn accepted(&mut self) -> Result<()> {
        Ok(())
    }
    fn push(&mut self, _rows: &[Row]) -> Result<()> {
        Err(Code::Unsupported)
    }
    /// Pure operation by contract: do not perform I/O or mutate business state.
    fn transform(&self, _row: &Row) -> Result<Vec<Row>> {
        Err(Code::Unsupported)
    }
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}
fn receive(input: &mut impl Read, cap: usize) -> Result<Option<Request>> {
    let mut header = [0; 4];
    match input.read(&mut header[..1]) {
        Ok(0) => return Ok(None),
        Ok(_) => {}
        Err(_) => return Err(Code::Io),
    };
    input
        .read_exact(&mut header[1..])
        .map_err(|_| Code::Protocol)?;
    let n = u32::from_be_bytes(header) as usize;
    if n == 0 || n > cap {
        return Err(Code::Limit);
    }
    let mut bytes = vec![0; n];
    input.read_exact(&mut bytes).map_err(|_| Code::Protocol)?;
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|_| Code::Protocol)
}
fn send(output: &mut impl Write, response: &Response, cap: usize) -> Result<()> {
    let bytes = encode(response, cap)?;
    output
        .write_all(&(bytes.len() as u32).to_be_bytes())
        .map_err(|_| Code::Io)?;
    output.write_all(&bytes).map_err(|_| Code::Io)?;
    output.flush().map_err(|_| Code::Io)
}
fn alarm(seconds: u32) {
    #[cfg(unix)]
    unsafe {
        libc::alarm(seconds);
    }
}
/// A private stdin/stdout framed server. Never log business data on stdout.
pub fn serve(factory: impl FnOnce(Role) -> Result<Box<dyn Plugin>>) -> Result<()> {
    if std::env::args().nth(1).as_deref() != Some("--sparrow-extension-v1") {
        return Err(Code::Protocol);
    }
    #[cfg(unix)]
    unsafe {
        let mut mask: libc::sigset_t = std::mem::zeroed();
        if libc::sigemptyset(&mut mask) != 0
            || libc::sigaddset(&mut mask, libc::SIGALRM) != 0
            || libc::sigprocmask(libc::SIG_UNBLOCK, &mask, std::ptr::null_mut()) != 0
            || libc::signal(libc::SIGALRM, libc::SIG_DFL) == libc::SIG_ERR
        {
            return Err(Code::Io);
        }
    }
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let mut input = stdin.lock();
    let mut output = stdout.lock();
    let first = receive(&mut input, MAX_FRAME)?.ok_or(Code::Protocol)?;
    let Operation::Describe { protocol, role } = first.operation else {
        return Err(Code::Protocol);
    };
    if protocol != PROTOCOL || first.sequence != 1 {
        return Err(Code::Protocol);
    }
    alarm(2);
    let mut plugin = factory(role)?;
    let declaration = plugin.declaration();
    declaration.validate()?;
    if declaration.role != role {
        return Err(Code::Protocol);
    }
    let cap = declaration.limits.max_frame_bytes;
    send(
        &mut output,
        &Response {
            sequence: 1,
            reply: Reply::Description {
                protocol: PROTOCOL.into(),
                declaration: declaration.clone(),
            },
        },
        MAX_FRAME,
    )?;
    alarm(0);
    let mut sequence = 1u64;
    let mut open = false;
    let mut ended = false;
    let mut pending = None;
    while let Some(request) = receive(&mut input, cap)? {
        sequence = sequence.checked_add(1).ok_or(Code::Limit)?;
        if request.sequence != sequence {
            return Err(Code::Protocol);
        }
        alarm(2);
        let reply = (|| -> Result<Reply> {
            Ok(match request.operation {
                Operation::Open { config } if !open => {
                    encode(&config, MAX_CONFIG)?;
                    plugin.open(&config)?;
                    open = true;
                    Reply::Opened
                }
                Operation::Poll { max_rows }
                    if open && !ended && role == Role::Source && pending.is_none() =>
                {
                    if max_rows == 0 || max_rows > declaration.limits.max_rows {
                        return Err(Code::Limit);
                    }
                    match plugin.poll(max_rows)? {
                        Poll::Data { rows, watermark } => {
                            validate_rows(&rows, &declaration.output, max_rows)?;
                            if (rows.is_empty() && watermark.is_none())
                                || (watermark.is_some() && !declaration.watermarks)
                            {
                                return Err(Code::Protocol);
                            }
                            pending = Some(sequence);
                            Reply::Data { rows, watermark }
                        }
                        Poll::Idle { retry_after_ms } if (1..=1000).contains(&retry_after_ms) => {
                            Reply::Idle { retry_after_ms }
                        }
                        Poll::Idle { .. } => return Err(Code::Limit),
                        Poll::End => {
                            ended = true;
                            Reply::End
                        }
                    }
                }
                Operation::Accepted { poll } if open && pending == Some(poll) => {
                    plugin.accepted()?;
                    pending = None;
                    Reply::Accepted
                }
                Operation::Push { rows } if open && role == Role::Sink => {
                    validate_rows(&rows, &declaration.input, declaration.limits.max_rows)?;
                    plugin.push(&rows)?;
                    Reply::Accepted
                }
                Operation::Transform { row } if open && role == Role::Transform => {
                    validate_rows(std::slice::from_ref(&row), &declaration.input, 1)?;
                    let rows = plugin.transform(&row)?;
                    validate_rows(&rows, &declaration.output, declaration.limits.max_rows)?;
                    Reply::Rows { rows }
                }
                Operation::Flush if open && role == Role::Sink => {
                    plugin.flush()?;
                    Reply::Flushed
                }
                Operation::Close => {
                    if open {
                        plugin.close()?;
                    }
                    Reply::Closed
                }
                _ => return Err(Code::Protocol),
            })
        })();
        let failed = reply.is_err();
        let reply = reply.unwrap_or_else(|code| Reply::Failure { code });
        let closed = matches!(reply, Reply::Closed);
        send(&mut output, &Response { sequence, reply }, cap)?;
        alarm(0);
        if failed {
            return Err(Code::Failed);
        }
        if closed {
            return Ok(());
        }
    }
    // Parent EOF is teardown, not an implied durable flush/commit.
    Ok(())
}
