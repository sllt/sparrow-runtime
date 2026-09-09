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

pub async fn read_packet(stream: &mut MqttStream) -> Result<Packet> {
    let mut first = [0u8; 1];
    stream
        .read_exact(&mut first)
        .await
        .map_err(|e| ConnectorError::new(ErrorCode::Internal, format!("MQTT read header: {e}")))?;
    let mut len_buf = Vec::new();
    loop {
        let mut b = [0u8; 1];
        stream
            .read_exact(&mut b)
            .await
            .map_err(|e| ConnectorError::new(ErrorCode::Internal, format!("MQTT read length: {e}")))?;
        len_buf.push(b[0]);
        if b[0] & 0x80 == 0 {
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
    let mut payload = vec![0u8; len];
    if len > 0 {
        stream
            .read_exact(&mut payload)
            .await
            .map_err(|e| ConnectorError::new(ErrorCode::Internal, format!("MQTT read payload: {e}")))?;
    }
    decode(first[0], &payload)
}
