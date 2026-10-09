//! Minimal RESP2 (Redis serialization protocol v2) for a client that sends
//! commands as arrays of bulk strings and reads replies with hard bounds.
//!
//! Replies are parsed from one fixed-capacity buffer allocated up front (its
//! size is charged by the caller). A bulk string longer than the configured
//! limit is rejected from its length header, before any byte of it is read
//! or buffered. RESP3 types are refused; a connection that produced a
//! protocol error must be discarded (its stream position is unknown).

use std::io;
use std::ops::Range;

use tokio::io::{AsyncRead, AsyncReadExt};

/// Longest status/error/integer/length line (without CRLF).
pub const MAX_LINE: usize = 4096;

/// Byte length of `$<len>\r\n<bytes>\r\n` for a bulk argument of `len`.
pub fn bulk_len(len: usize) -> usize {
    1 + digits(len as u64) + 2 + len + 2
}

/// Byte length of the `*<n>\r\n` array header.
pub fn array_header_len(n: usize) -> usize {
    1 + digits(n as u64) + 2
}

pub fn digits(mut n: u64) -> usize {
    let mut d = 1;
    while n >= 10 {
        n /= 10;
        d += 1;
    }
    d
}

/// Write `n` in decimal into `out` (no allocation beyond `out`'s spare
/// capacity, which callers reserve exactly).
pub fn push_decimal(out: &mut Vec<u8>, n: u64) {
    let mut buf = [0u8; 20];
    let mut i = buf.len();
    let mut n = n;
    loop {
        i -= 1;
        buf[i] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    out.extend_from_slice(&buf[i..]);
}

pub fn push_array_header(out: &mut Vec<u8>, n: usize) {
    out.push(b'*');
    push_decimal(out, n as u64);
    out.extend_from_slice(b"\r\n");
}

pub fn push_bulk_header(out: &mut Vec<u8>, len: usize) {
    out.push(b'$');
    push_decimal(out, len as u64);
    out.extend_from_slice(b"\r\n");
}

/// A whole command of plain arguments (handshake / lookup requests).
pub fn command_len(args: &[&[u8]]) -> usize {
    args.iter()
        .fold(array_header_len(args.len()), |n, a| n + bulk_len(a.len()))
}

pub fn push_command(out: &mut Vec<u8>, args: &[&[u8]]) {
    push_array_header(out, args.len());
    for arg in args {
        push_bulk_header(out, arg.len());
        out.extend_from_slice(arg);
        out.extend_from_slice(b"\r\n");
    }
}

/// One RESP2 reply element. Ranges index into [`Reader::bytes`] and are
/// valid only until the next call to [`Reader::next`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Frame {
    Simple(Range<usize>),
    Error(Range<usize>),
    Integer(i64),
    Bulk(Option<Range<usize>>),
    /// Header of an array; its elements follow as separate frames.
    Array(Option<usize>),
}

#[derive(Debug)]
pub enum ReadError {
    Io(io::Error),
    /// Peer closed the connection.
    Closed,
    /// Malformed, RESP3 or over-limit reply: discard the connection.
    Protocol(&'static str),
}

impl From<io::Error> for ReadError {
    fn from(e: io::Error) -> Self {
        ReadError::Io(e)
    }
}

pub struct Reader {
    buf: Box<[u8]>,
    start: usize,
    end: usize,
    max_bulk: usize,
    max_array: usize,
}

impl Reader {
    /// Buffer bytes needed for replies with bulk strings up to `max_bulk`.
    pub fn capacity_for(max_bulk: usize) -> usize {
        max_bulk.saturating_add(MAX_LINE).saturating_add(64)
    }

    /// Allocates `capacity_for(max_bulk)` bytes once.
    pub fn new(max_bulk: usize, max_array: usize) -> Self {
        Self {
            buf: vec![0u8; Self::capacity_for(max_bulk)].into_boxed_slice(),
            start: 0,
            end: 0,
            max_bulk,
            max_array,
        }
    }

    pub fn bytes(&self, range: Range<usize>) -> &[u8] {
        &self.buf[range]
    }

    /// Forget buffered bytes (for a new connection).
    pub fn reset(&mut self) {
        self.start = 0;
        self.end = 0;
    }

    /// Bytes received but not yet parsed (a reply the caller did not ask for).
    pub fn has_buffered(&self) -> bool {
        self.start < self.end
    }

    /// Read the next frame. Never reads past what is needed for it plus
    /// whatever the socket already delivered in the same read.
    pub async fn next<R: AsyncRead + Unpin + ?Sized>(
        &mut self,
        io: &mut R,
    ) -> Result<Frame, ReadError> {
        loop {
            match self.parse()? {
                Some(frame) => return Ok(frame),
                None => self.fill(io).await?,
            }
        }
    }

    async fn fill<R: AsyncRead + Unpin + ?Sized>(&mut self, io: &mut R) -> Result<(), ReadError> {
        if self.start > 0 {
            self.buf.copy_within(self.start..self.end, 0);
            self.end -= self.start;
            self.start = 0;
        }
        if self.end == self.buf.len() {
            return Err(ReadError::Protocol("Redis reply exceeds the read buffer"));
        }
        let n = io.read(&mut self.buf[self.end..]).await?;
        if n == 0 {
            return Err(ReadError::Closed);
        }
        self.end += n;
        Ok(())
    }

    /// Parse one frame from the buffer, `None` if more bytes are needed.
    fn parse(&mut self) -> Result<Option<Frame>, ReadError> {
        let data = &self.buf[self.start..self.end];
        let Some(&kind) = data.first() else {
            return Ok(None);
        };
        let window = &data[1..data.len().min(MAX_LINE + 2)];
        let Some(eol) = window.windows(2).position(|w| w == b"\r\n") else {
            if data.len() > MAX_LINE + 2 {
                return Err(ReadError::Protocol("Redis reply line exceeds 4096 bytes"));
            }
            return Ok(None);
        };
        let line = self.start + 1..self.start + 1 + eol;
        let after = self.start + 1 + eol + 2;
        let frame = match kind {
            b'+' => Frame::Simple(line),
            b'-' => Frame::Error(line),
            b':' => Frame::Integer(parse_i64(&self.buf[line])?),
            b'$' => {
                let n = parse_i64(&self.buf[line])?;
                if n == -1 {
                    Frame::Bulk(None)
                } else if n < 0 {
                    return Err(ReadError::Protocol("negative Redis bulk length"));
                } else if n as u64 > self.max_bulk as u64 {
                    // Rejected from the header: nothing of it is buffered.
                    return Err(ReadError::Protocol("Redis bulk reply exceeds its limit"));
                } else {
                    let n = n as usize;
                    if self.end - after < n + 2 {
                        return Ok(None);
                    }
                    if &self.buf[after + n..after + n + 2] != b"\r\n" {
                        return Err(ReadError::Protocol("Redis bulk reply not CRLF-terminated"));
                    }
                    self.start = after + n + 2;
                    return Ok(Some(Frame::Bulk(Some(after..after + n))));
                }
            }
            b'*' => {
                let n = parse_i64(&self.buf[line])?;
                if n == -1 {
                    Frame::Array(None)
                } else if n < 0 || n as u64 > self.max_array as u64 {
                    return Err(ReadError::Protocol("Redis array reply exceeds its limit"));
                } else {
                    Frame::Array(Some(n as usize))
                }
            }
            _ => {
                return Err(ReadError::Protocol(
                    "unexpected Redis reply type (RESP2 only)",
                ))
            }
        };
        self.start = after;
        Ok(Some(frame))
    }
}

fn parse_i64(line: &[u8]) -> Result<i64, ReadError> {
    let bad = || ReadError::Protocol("invalid Redis integer");
    let (neg, digits) = match line.split_first() {
        Some((b'-', rest)) => (true, rest),
        _ => (false, line),
    };
    if digits.is_empty() || digits.len() > 19 || !digits.iter().all(u8::is_ascii_digit) {
        return Err(bad());
    }
    let mut v: i64 = 0;
    for &d in digits {
        v = v
            .checked_mul(10)
            .and_then(|v| v.checked_add(i64::from(d - b'0')))
            .ok_or_else(bad)?;
    }
    Ok(if neg { -v } else { v })
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn frames(input: &[u8], max_bulk: usize) -> Vec<Result<Frame, String>> {
        let mut reader = Reader::new(max_bulk, 4);
        let mut io = input;
        let mut out = Vec::new();
        loop {
            match reader.next(&mut io).await {
                Ok(f) => {
                    let shown = match &f {
                        Frame::Simple(r) | Frame::Error(r) => {
                            String::from_utf8_lossy(reader.bytes(r.clone())).to_string()
                        }
                        Frame::Bulk(Some(r)) => {
                            String::from_utf8_lossy(reader.bytes(r.clone())).to_string()
                        }
                        _ => String::new(),
                    };
                    out.push(Ok(f.clone()));
                    if !shown.is_empty() {
                        out.push(Err(format!("text:{shown}")));
                    }
                }
                Err(ReadError::Closed) => break,
                Err(ReadError::Protocol(m)) => {
                    out.push(Err(m.to_string()));
                    break;
                }
                Err(ReadError::Io(e)) => panic!("{e}"),
            }
        }
        out
    }

    #[test]
    fn encodes_commands_with_exact_lengths() {
        let args: [&[u8]; 3] = [b"SET", b"k", b"v\r\n"];
        let mut out = Vec::new();
        push_command(&mut out, &args);
        assert_eq!(out, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$3\r\nv\r\n\r\n");
        assert_eq!(command_len(&args), out.len());
        assert_eq!(bulk_len(0), b"$0\r\n\r\n".len());
        assert_eq!(digits(0), 1);
        assert_eq!(digits(u64::MAX), 20);
        let mut n = Vec::new();
        push_decimal(&mut n, u64::MAX);
        assert_eq!(n, u64::MAX.to_string().as_bytes());
        assert_eq!(array_header_len(10), 5);
    }

    #[tokio::test]
    async fn parses_every_resp2_type_across_short_reads() {
        let input = b"+OK\r\n-WRONGTYPE Operation against a key\r\n:-42\r\n$5\r\nhe\r\no\r\n$-1\r\n*2\r\n$0\r\n\r\n*-1\r\n";
        let got = frames(input, 16).await;
        assert_eq!(
            got,
            vec![
                Ok(Frame::Simple(1..3)),
                Err("text:OK".into()),
                Ok(Frame::Error(6..39)),
                Err("text:WRONGTYPE Operation against a key".into()),
                Ok(Frame::Integer(-42)),
                Ok(Frame::Bulk(Some(51..56))),
                Err("text:he\r\no".into()),
                Ok(Frame::Bulk(None)),
                Ok(Frame::Array(Some(2))),
                Ok(Frame::Bulk(Some(71..71))),
                Ok(Frame::Array(None)),
            ]
        );
        // Byte-at-a-time delivery yields the same frames.
        let mut reader = Reader::new(16, 4);
        let (mut client, mut server) = tokio::io::duplex(1);
        tokio::spawn(async move {
            use tokio::io::AsyncWriteExt;
            for b in b"$5\r\nhello\r\n:7\r\n" {
                server.write_all(&[*b]).await.unwrap();
            }
        });
        let f = reader.next(&mut client).await.unwrap();
        let Frame::Bulk(Some(r)) = f else { panic!() };
        assert_eq!(reader.bytes(r), b"hello");
        assert_eq!(reader.next(&mut client).await.unwrap(), Frame::Integer(7));
    }

    #[tokio::test]
    async fn rejects_oversize_malformed_and_resp3_before_buffering() {
        // Bulk header above the limit is refused without reading the body.
        let got = frames(b"$17\r\n", 16).await;
        assert_eq!(got, vec![Err("Redis bulk reply exceeds its limit".into())]);
        let got = frames(format!("${}\r\n", u64::MAX).as_bytes(), 16).await;
        assert_eq!(got, vec![Err("invalid Redis integer".into())]);
        for (input, why) in [
            (&b"*5\r\n"[..], "Redis array reply exceeds its limit"),
            (b"$-2\r\n", "negative Redis bulk length"),
            (b":12a\r\n", "invalid Redis integer"),
            (b":\r\n", "invalid Redis integer"),
            (b"$2\r\nabcd", "Redis bulk reply not CRLF-terminated"),
            (b"_\r\n", "unexpected Redis reply type (RESP2 only)"),
            (b"%1\r\n", "unexpected Redis reply type (RESP2 only)"),
        ] {
            assert_eq!(frames(input, 16).await, vec![Err(why.into())], "{input:?}");
        }
        let long = [b"+".as_slice(), &vec![b'x'; MAX_LINE + 8]].concat();
        assert_eq!(
            frames(&long, 16).await,
            vec![Err("Redis reply line exceeds 4096 bytes".into())]
        );
        assert_eq!(
            parse_i64(b"-9223372036854775808").ok(),
            None,
            "no i64::MIN overflow trick"
        );
        assert_eq!(parse_i64(b"9223372036854775807").ok(), Some(i64::MAX));
    }
}
