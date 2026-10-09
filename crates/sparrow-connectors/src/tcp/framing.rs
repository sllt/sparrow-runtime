//! Incremental, allocation-free stream framing for TCP: newline-delimited
//! records (`lines`) or length-prefixed frames (`u16`/`u32` big endian).
//!
//! One buffer of `max_frame_bytes + prefix/terminator + READ_CHUNK` is
//! allocated per connection and never grows. Limits are enforced before a
//! record is handed to a decoder:
//!
//! - `lines`: a line (without `\n` / `\r\n`) longer than the limit is
//!   detected as soon as `limit + 2` bytes arrive without a newline;
//! - `length_prefixed`: a declared length above the limit is rejected from
//!   the prefix alone; the payload is skipped (`resync`) without buffering.
//!
//! The reader is cancel safe: state changes only happen synchronously after
//! a completed read.

use std::ops::Range;

use tokio::io::{AsyncRead, AsyncReadExt};

/// Bytes requested from the socket per read.
pub(crate) const READ_CHUNK: usize = 16 * 1024;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TcpFraming {
    /// One record per `\n`-terminated line (`\r\n` accepted); blank lines are
    /// skipped.
    #[default]
    Lines,
    /// One record per frame: a big-endian length prefix, then that many bytes.
    LengthPrefixed,
}

/// Width of the length prefix for `length_prefixed`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PrefixWidth {
    U16,
    #[default]
    U32,
}

impl PrefixWidth {
    pub fn bytes(self) -> usize {
        match self {
            Self::U16 => 2,
            Self::U32 => 4,
        }
    }

    /// Largest length the prefix can express.
    pub fn max_len(self) -> usize {
        match self {
            Self::U16 => u16::MAX as usize,
            Self::U32 => u32::MAX as usize,
        }
    }
}

/// What the Source does with an oversize record.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OversizePolicy {
    /// Skip to the next record boundary (next newline / past the declared
    /// payload) and keep the connection.
    #[default]
    Resync,
    /// Close the connection (and reconnect).
    Disconnect,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Frame {
    /// A complete record at this range of [`FrameReader::bytes`].
    Record(Range<usize>),
    /// A record over the limit; with `Resync` the reader already skips it.
    Oversize,
}

pub(crate) struct FrameReader {
    framing: TcpFraming,
    width: PrefixWidth,
    limit: usize,
    oversize: OversizePolicy,
    skip_whitespace_lines: bool,
    buf: Box<[u8]>,
    start: usize,
    end: usize,
    /// `lines`: bytes of `start..end` already searched for `\n`.
    scanned: usize,
    /// `lines` resync: dropping bytes until the next newline.
    discard_line: bool,
    /// `length_prefixed` resync: payload bytes still to skip.
    skip: u64,
}

impl FrameReader {
    /// `limit` is the record limit (already capped to the prefix range).
    pub(crate) fn new(
        framing: TcpFraming,
        width: PrefixWidth,
        limit: usize,
        oversize: OversizePolicy,
    ) -> Self {
        Self {
            framing,
            width,
            limit,
            oversize,
            skip_whitespace_lines: true,
            buf: vec![0u8; Self::capacity(limit, width)].into_boxed_slice(),
            start: 0,
            end: 0,
            scanned: 0,
            discard_line: false,
            skip: 0,
        }
    }

    /// The fixed per-connection buffer size (saturating; `limit` is
    /// validated to <= 64 KiB before a reader is built).
    pub(crate) fn capacity(limit: usize, width: PrefixWidth) -> usize {
        limit
            .saturating_add(width.bytes().max(2))
            .saturating_add(READ_CHUNK)
    }

    pub(crate) fn bytes(&self, range: Range<usize>) -> &[u8] {
        &self.buf[range]
    }

    /// CSV treats spaces/tabs as field data, not a blank document line.
    pub(crate) fn preserve_whitespace_lines(&mut self) {
        self.skip_whitespace_lines = false;
    }

    /// Bytes of an unfinished record (or a skip in progress) at EOF.
    pub(crate) fn has_partial(&self) -> bool {
        self.end > self.start || self.discard_line || self.skip > 0
    }

    /// Read once into the free tail (compacting first). `Ok(0)` = EOF.
    pub(crate) async fn fill<R: AsyncRead + Unpin + ?Sized>(
        &mut self,
        io: &mut R,
    ) -> std::io::Result<usize> {
        if self.buf.len() - self.end < READ_CHUNK && self.start > 0 {
            self.buf.copy_within(self.start..self.end, 0);
            self.end -= self.start;
            // `scanned` is relative to `start`, so it survives the move.
            self.start = 0;
        }
        debug_assert!(self.end < self.buf.len(), "drain frames before reading");
        let n = io.read(&mut self.buf[self.end..]).await?;
        self.end += n;
        Ok(n)
    }

    /// Test helper: append bytes as if read from the socket.
    #[cfg(test)]
    pub(crate) fn push(&mut self, bytes: &[u8]) {
        for chunk in bytes.chunks(READ_CHUNK) {
            if self.buf.len() - self.end < chunk.len() {
                self.buf.copy_within(self.start..self.end, 0);
                self.end -= self.start;
                self.scanned = 0;
                self.start = 0;
            }
            self.buf[self.end..self.end + chunk.len()].copy_from_slice(chunk);
            self.end += chunk.len();
        }
    }

    /// Next complete frame from buffered bytes; `None` = need more input.
    pub(crate) fn next_frame(&mut self) -> Option<Frame> {
        match self.framing {
            TcpFraming::Lines => self.next_line(),
            TcpFraming::LengthPrefixed => self.next_prefixed(),
        }
    }

    fn next_line(&mut self) -> Option<Frame> {
        loop {
            let from = self.start + self.scanned;
            let newline = self.buf[from..self.end].iter().position(|&b| b == b'\n');
            if self.discard_line {
                match newline {
                    Some(i) => {
                        self.start = from + i + 1;
                        self.scanned = 0;
                        self.discard_line = false;
                        continue;
                    }
                    None => {
                        self.start = self.end;
                        self.scanned = 0;
                        return None;
                    }
                }
            }
            let Some(i) = newline else {
                self.scanned = self.end - self.start;
                // `limit` content bytes plus an optional `\r` and no newline
                // yet: the line can no longer fit.
                if self.end - self.start > self.limit + 1 {
                    return Some(self.oversize_line());
                }
                return None;
            };
            let line_start = self.start;
            let mut line_end = from + i;
            self.start = line_end + 1;
            self.scanned = 0;
            if line_end > line_start && self.buf[line_end - 1] == b'\r' {
                line_end -= 1;
            }
            if line_end - line_start > self.limit {
                return Some(Frame::Oversize);
            }
            // Blank = only JSON whitespace (space, tab, CR; LF ends the
            // line). Form feed / vertical tab are record bytes.
            if line_start == line_end
                || (self.skip_whitespace_lines
                    && self.buf[line_start..line_end]
                        .iter()
                        .all(|b| matches!(b, b' ' | b'\t' | b'\r')))
            {
                continue;
            }
            return Some(Frame::Record(line_start..line_end));
        }
    }

    fn oversize_line(&mut self) -> Frame {
        if self.oversize == OversizePolicy::Resync {
            self.discard_line = true;
            self.start = self.end;
            self.scanned = 0;
        }
        Frame::Oversize
    }

    fn next_prefixed(&mut self) -> Option<Frame> {
        if self.skip > 0 {
            let n = (self.end - self.start).min(self.skip.min(usize::MAX as u64) as usize);
            self.start += n;
            self.skip -= n as u64;
            if self.skip > 0 {
                return None;
            }
        }
        let width = self.width.bytes();
        if self.end - self.start < width {
            return None;
        }
        let prefix = &self.buf[self.start..self.start + width];
        let len = match self.width {
            PrefixWidth::U16 => u16::from_be_bytes([prefix[0], prefix[1]]) as usize,
            PrefixWidth::U32 => {
                u32::from_be_bytes([prefix[0], prefix[1], prefix[2], prefix[3]]) as usize
            }
        };
        if len > self.limit {
            // Rejected from the prefix alone; never buffered.
            if self.oversize == OversizePolicy::Resync {
                self.start += width;
                self.skip = len as u64;
                if self.start == self.end {
                    self.start = 0;
                    self.end = 0;
                }
            }
            return Some(Frame::Oversize);
        }
        if self.end - self.start - width < len {
            return None;
        }
        let record = self.start + width..self.start + width + len;
        self.start = record.end;
        Some(Frame::Record(record))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn drain(r: &mut FrameReader) -> Vec<Result<Vec<u8>, ()>> {
        let mut out = Vec::new();
        while let Some(frame) = r.next_frame() {
            out.push(match frame {
                Frame::Record(range) => Ok(r.bytes(range).to_vec()),
                Frame::Oversize => Err(()),
            });
        }
        out
    }

    fn ok(s: &str) -> Result<Vec<u8>, ()> {
        Ok(s.as_bytes().to_vec())
    }

    #[test]
    fn csv_preserves_space_tab_and_bare_cr_records() {
        let mut r = FrameReader::new(
            TcpFraming::Lines,
            PrefixWidth::U32,
            16,
            OversizePolicy::Resync,
        );
        r.preserve_whitespace_lines();
        r.push(b" \n\t\n\r\r\n\n\r\n");
        assert_eq!(drain(&mut r), vec![ok(" "), ok("\t"), ok("\r")]);
    }

    #[test]
    fn lines_crlf_blank_fragmented_and_coalesced() {
        let input = b"a\r\n\r\n  \nbb\ncc\r\nd";
        // Coalesced: one push.
        let mut r = FrameReader::new(
            TcpFraming::Lines,
            PrefixWidth::U32,
            4,
            OversizePolicy::Resync,
        );
        r.push(input);
        assert_eq!(drain(&mut r), vec![ok("a"), ok("bb"), ok("cc")]);
        assert!(r.has_partial(), "unterminated `d` stays buffered");
        // Fragmented: byte by byte gives the same records.
        let mut r = FrameReader::new(
            TcpFraming::Lines,
            PrefixWidth::U32,
            4,
            OversizePolicy::Resync,
        );
        let mut got = Vec::new();
        for b in input {
            r.push(&[*b]);
            got.extend(drain(&mut r));
        }
        assert_eq!(got, vec![ok("a"), ok("bb"), ok("cc")]);
    }

    #[test]
    fn lines_blank_is_json_whitespace_only() {
        let mut r = FrameReader::new(
            TcpFraming::Lines,
            PrefixWidth::U32,
            8,
            OversizePolicy::Resync,
        );
        r.push(b" \t\r\n\x0c\n\x0b\nok\n");
        assert_eq!(drain(&mut r), vec![ok("\x0c"), ok("\x0b"), ok("ok")]);
    }

    #[tokio::test]
    async fn lines_scan_position_survives_compaction() {
        // A long unterminated line whose newline arrives after the buffer
        // was compacted by `fill` is still one record.
        let mut r = FrameReader::new(
            TcpFraming::Lines,
            PrefixWidth::U32,
            48 * 1024,
            OversizePolicy::Resync,
        );
        let body = vec![b'x'; 45 * 1024];
        let mut first = b"a\n".repeat(5000);
        first.extend(&body);
        let mut src: &[u8] = &first;
        while !src.is_empty() {
            r.fill(&mut src).await.unwrap();
            assert!(drain(&mut r).iter().all(|f| f == &ok("a")));
        }
        assert!(
            r.start > 0 && r.buf.len() - r.end < READ_CHUNK,
            "compaction due"
        );
        let mut src: &[u8] = b"\n";
        r.fill(&mut src).await.unwrap();
        assert_eq!(r.start, 0, "compacted");
        assert_eq!(drain(&mut r), vec![Ok(body)]);
        assert!(!r.has_partial());
    }

    #[test]
    fn lines_oversize_resync_and_disconnect() {
        let mut r = FrameReader::new(
            TcpFraming::Lines,
            PrefixWidth::U32,
            4,
            OversizePolicy::Resync,
        );
        // Exactly at the limit (with CRLF) fits; one more byte does not.
        r.push(b"abcd\r\nabcde\nok\n");
        assert_eq!(drain(&mut r), vec![ok("abcd"), Err(()), ok("ok")]);
        // A long line without a newline is detected early and skipped across
        // reads without buffering it.
        r.push(b"xxxxxxxx");
        assert_eq!(drain(&mut r), vec![Err(())]);
        for _ in 0..100 {
            r.push(&[b'y'; 1000]);
            assert_eq!(drain(&mut r), vec![]);
        }
        r.push(b"zz\nnext\n");
        assert_eq!(drain(&mut r), vec![ok("next")]);
        assert!(!r.has_partial());

        let mut r = FrameReader::new(
            TcpFraming::Lines,
            PrefixWidth::U32,
            4,
            OversizePolicy::Disconnect,
        );
        r.push(b"ok\nxxxxxx");
        assert_eq!(r.next_frame(), Some(Frame::Record(0..2)));
        assert_eq!(r.next_frame(), Some(Frame::Oversize));
    }

    #[test]
    fn length_prefixed_widths_fragmentation_and_oversize_skip() {
        let mut wire = Vec::new();
        for payload in [&b"one"[..], b"", b"three"] {
            wire.extend((payload.len() as u32).to_be_bytes());
            wire.extend(payload);
        }
        // Declared 4 GiB - 1: rejected from the prefix, skipped lazily.
        wire.extend(u32::MAX.to_be_bytes());
        let mut r = FrameReader::new(
            TcpFraming::LengthPrefixed,
            PrefixWidth::U32,
            8,
            OversizePolicy::Resync,
        );
        let mut got = Vec::new();
        for b in &wire {
            r.push(&[*b]);
            got.extend(drain(&mut r));
        }
        assert_eq!(got, vec![ok("one"), ok(""), ok("three"), Err(())]);
        assert!(r.has_partial(), "the skip is still pending");
        assert_eq!(
            r.buf.len(),
            FrameReader::capacity(8, PrefixWidth::U32),
            "never grows"
        );

        let mut r = FrameReader::new(
            TcpFraming::LengthPrefixed,
            PrefixWidth::U16,
            8,
            OversizePolicy::Resync,
        );
        r.push(&[0, 9]);
        r.push(b"123456789");
        r.push(&[0, 2, b'o', b'k']);
        assert_eq!(drain(&mut r), vec![Err(()), ok("ok")]);
        assert!(!r.has_partial());
    }
}
