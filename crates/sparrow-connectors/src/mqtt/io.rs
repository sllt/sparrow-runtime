use std::pin::Pin;
use std::time::Duration;

use sparrow_model::ErrorCode;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

use super::codec::{decode, decode_remaining_length, encode, Packet, MAX_PACKET_BYTES};
use crate::error::{ConnectorError, Result};
use crate::tls::TlsConfig;

pub type MqttStream = Pin<Box<dyn MqttIo>>;

pub trait MqttIo: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T> MqttIo for T where T: AsyncRead + AsyncWrite + Send + Unpin {}

pub async fn connect_plain(host: &str, port: u16, tls: &TlsConfig, wait: Duration) -> Result<MqttStream> {
    tls.validate()?;
    if tls.enabled {
        return Err(ConnectorError::new(
            ErrorCode::FeatureUnavailable,
            "MQTT TLS transport is not wired in this demo build; enable TLS only on the HTTP sink (reqwest rustls verifies certificates). skip_verify is still rejected",
        ));
    }
    let addr = format!("{host}:{port}");
    let stream = timeout(wait, TcpStream::connect(&addr))
        .await
        .map_err(|_| ConnectorError::new(ErrorCode::Internal, format!("MQTT connect timeout to {addr}")))?
        .map_err(|e| ConnectorError::new(ErrorCode::Internal, format!("MQTT connect {addr}: {e}")))?;
    let _ = stream.set_nodelay(true);
    Ok(Box::pin(stream))
}

pub async fn write_packet(stream: &mut MqttStream, packet: &Packet) -> Result<()> {
    let bytes = encode(packet)?;
    stream
        .write_all(&bytes)
        .await
        .map_err(|e| ConnectorError::new(ErrorCode::Internal, format!("MQTT write: {e}")))?;
    stream
        .flush()
        .await
        .map_err(|e| ConnectorError::new(ErrorCode::Internal, format!("MQTT flush: {e}")))
}

/// Cancel-safe framed reader. Bytes already pulled from the socket stay in
/// `buf` when a `select!` branch (ping / cancel) wins (R19).
#[derive(Default)]
pub struct MqttFramedReader {
    buf: Vec<u8>,
}

impl MqttFramedReader {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn next(&mut self, stream: &mut MqttStream) -> Result<Packet> {
        loop {
            if let Some(pkt) = try_decode_frame(&mut self.buf)? {
                return Ok(pkt);
            }
            let mut tmp = [0u8; 1024];
            let n = stream.read(&mut tmp).await.map_err(|e| {
                ConnectorError::new(ErrorCode::Internal, format!("MQTT read: {e}"))
            })?;
            if n == 0 {
                return Err(ConnectorError::new(
                    ErrorCode::Internal,
                    "MQTT connection closed",
                ));
            }
            self.buf.extend_from_slice(&tmp[..n]);
            if self.buf.len() > MAX_PACKET_BYTES + 8 {
                return Err(ConnectorError::new(
                    ErrorCode::MaxRecordSize,
                    "MQTT framed buffer exceeded",
                ));
            }
        }
    }
}

fn try_decode_frame(buf: &mut Vec<u8>) -> Result<Option<Packet>> {
    if buf.is_empty() {
        return Ok(None);
    }
    let first = buf[0];
    let mut used = 1usize;
    let mut len_buf = Vec::new();
    loop {
        if used >= buf.len() {
            return Ok(None);
        }
        let b = buf[used];
        used += 1;
        len_buf.push(b);
        if b & 0x80 == 0 {
            break;
        }
        if len_buf.len() > 4 {
            return Err(ConnectorError::new(
                ErrorCode::CodecViolation,
                "MQTT remaining length too long",
            ));
        }
    }
    let (len, _) = decode_remaining_length(&len_buf)?;
    if len > MAX_PACKET_BYTES {
        return Err(ConnectorError::new(
            ErrorCode::MaxRecordSize,
            format!("MQTT packet {len}B exceeds {MAX_PACKET_BYTES}"),
        ));
    }
    if buf.len() < used + len {
        return Ok(None);
    }
    let payload = buf[used..used + len].to_vec();
    let pkt = decode(first, &payload)?;
    buf.drain(..used + len);
    Ok(Some(pkt))
}

pub async fn read_packet(stream: &mut MqttStream) -> Result<Packet> {
    let mut reader = MqttFramedReader::new();
    reader.next(stream).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mqtt::codec::encode;
    use crate::mqtt::codec::Packet;

    #[test]
    fn r19_partial_frame_is_retained() {
        let pkt = Packet::PingResp;
        let bytes = encode(&pkt).unwrap();
        let mut buf = bytes[..1].to_vec();
        assert!(try_decode_frame(&mut buf).unwrap().is_none());
        assert_eq!(buf.len(), 1, "half-frame must stay in the reader buffer");
        buf.extend_from_slice(&bytes[1..]);
        let got = try_decode_frame(&mut buf).unwrap().unwrap();
        assert!(matches!(got, Packet::PingResp));
        assert!(buf.is_empty());
    }
}
