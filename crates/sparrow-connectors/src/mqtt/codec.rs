//! MQTT 3.1.1 codec, QoS 0 focused. Remaining length is capped so a
//! misbehaving peer cannot grow an unbounded buffer.

use sparrow_model::ErrorCode;

use crate::error::{ConnectorError, Result};

pub const MAX_PACKET_BYTES: usize = 128 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Packet {
    Connect(Connect),
    ConnAck { session_present: bool, return_code: u8 },
    Publish(Publish),
    Subscribe { packet_id: u16, topics: Vec<(String, u8)> },
    SubAck { packet_id: u16, codes: Vec<u8> },
    PingReq,
    PingResp,
    Disconnect,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Connect {
    pub client_id: String,
    pub clean_session: bool,
    pub keepalive: u16,
    pub username: Option<String>,
    pub password: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Publish {
    pub dup: bool,
    pub qos: u8,
    pub retain: bool,
    pub topic: String,
    pub packet_id: Option<u16>,
    pub payload: Vec<u8>,
}

pub fn encode(packet: &Packet) -> Result<Vec<u8>> {
    match packet {
        Packet::Connect(c) => encode_connect(c),
        Packet::ConnAck {
            session_present,
            return_code,
        } => Ok(vec![
            0x20,
            0x02,
            u8::from(*session_present),
            *return_code,
        ]),
        Packet::Publish(p) => encode_publish(p),
        Packet::Subscribe { packet_id, topics } => encode_subscribe(*packet_id, topics),
        Packet::SubAck { packet_id, codes } => {
            let mut vh = packet_id.to_be_bytes().to_vec();
            vh.extend_from_slice(codes);
            finish(0x90, vh)
        }
        Packet::PingReq => Ok(vec![0xC0, 0x00]),
        Packet::PingResp => Ok(vec![0xD0, 0x00]),
        Packet::Disconnect => Ok(vec![0xE0, 0x00]),
    }
}

pub fn decode(first: u8, payload: &[u8]) -> Result<Packet> {
    let kind = first >> 4;
    match kind {
        1 => Ok(Packet::Connect(decode_connect(payload)?)),
        2 => {
            if payload.len() < 2 {
                return Err(bad("short CONNACK"));
            }
            Ok(Packet::ConnAck {
                session_present: payload[0] & 0x01 != 0,
                return_code: payload[1],
            })
        }
        3 => Ok(Packet::Publish(decode_publish(first, payload)?)),
        8 => decode_subscribe(payload),
        9 => {
            if payload.len() < 2 {
                return Err(bad("short SUBACK"));
            }
            Ok(Packet::SubAck {
                packet_id: u16::from_be_bytes([payload[0], payload[1]]),
                codes: payload[2..].to_vec(),
            })
        }
        12 => Ok(Packet::PingReq),
        13 => Ok(Packet::PingResp),
        14 => Ok(Packet::Disconnect),
        other => Err(bad(format!("unsupported MQTT packet type {other}"))),
    }
}

fn encode_connect(c: &Connect) -> Result<Vec<u8>> {
    let mut flags = 0u8;
    if c.clean_session {
        flags |= 0x02;
    }
    if c.username.is_some() {
        flags |= 0x80;
    }
    if c.password.is_some() {
        flags |= 0x40;
    }
    let mut vh = Vec::new();
    write_utf8(&mut vh, "MQTT");
    vh.push(4);
    vh.push(flags);
    vh.extend_from_slice(&c.keepalive.to_be_bytes());
    write_utf8(&mut vh, &c.client_id);
    if let Some(u) = &c.username {
        write_utf8(&mut vh, u);
    }
    if let Some(p) = &c.password {
        write_bytes(&mut vh, p);
    }
    finish(0x10, vh)
}

fn decode_connect(payload: &[u8]) -> Result<Connect> {
    let mut i = 0usize;
    let proto = read_utf8(payload, &mut i)?;
    if proto != "MQTT" {
        return Err(bad(format!("protocol name {proto}")));
    }
    if i >= payload.len() {
        return Err(bad("short CONNECT"));
    }
    let _level = payload[i];
    i += 1;
    if i + 2 >= payload.len() {
        return Err(bad("short CONNECT flags"));
    }
    let flags = payload[i];
    i += 1;
    let keepalive = u16::from_be_bytes([payload[i], payload[i + 1]]);
    i += 2;
    let client_id = read_utf8(payload, &mut i)?;
    if flags & 0x04 != 0 {
        let _ = read_utf8(payload, &mut i)?;
        let _ = read_bytes(payload, &mut i)?;
    }
    let username = if flags & 0x80 != 0 {
        Some(read_utf8(payload, &mut i)?)
    } else {
        None
    };
    let password = if flags & 0x40 != 0 {
        Some(read_bytes(payload, &mut i)?)
    } else {
        None
    };
    Ok(Connect {
        client_id,
        clean_session: flags & 0x02 != 0,
        keepalive,
        username,
        password,
    })
}

fn encode_publish(p: &Publish) -> Result<Vec<u8>> {
    if p.qos > 2 {
        return Err(bad("invalid publish qos"));
    }
    let mut flags = 3u8 << 4;
    if p.dup {
        flags |= 0x08;
    }
    flags |= (p.qos & 0x03) << 1;
    if p.retain {
        flags |= 0x01;
    }
    let mut vh = Vec::new();
    write_utf8(&mut vh, &p.topic);
    if p.qos > 0 {
        let id = p.packet_id.unwrap_or(1);
        vh.extend_from_slice(&id.to_be_bytes());
    }
    vh.extend_from_slice(&p.payload);
    finish(flags, vh)
}

fn decode_publish(first: u8, payload: &[u8]) -> Result<Publish> {
    let mut i = 0usize;
    let topic = read_utf8(payload, &mut i)?;
    let qos = (first >> 1) & 0x03;
    let packet_id = if qos > 0 {
        if i + 1 >= payload.len() {
            return Err(bad("short PUBLISH id"));
        }
        let id = u16::from_be_bytes([payload[i], payload[i + 1]]);
        i += 2;
        Some(id)
    } else {
        None
    };
    Ok(Publish {
        dup: first & 0x08 != 0,
        qos,
        retain: first & 0x01 != 0,
        topic,
        packet_id,
        payload: payload[i..].to_vec(),
    })
}

fn encode_subscribe(packet_id: u16, topics: &[(String, u8)]) -> Result<Vec<u8>> {
    let mut vh = packet_id.to_be_bytes().to_vec();
    for (t, qos) in topics {
        write_utf8(&mut vh, t);
        vh.push(*qos);
    }
    finish(0x82, vh)
}

fn decode_subscribe(payload: &[u8]) -> Result<Packet> {
    if payload.len() < 2 {
        return Err(bad("short SUBSCRIBE"));
    }
    let packet_id = u16::from_be_bytes([payload[0], payload[1]]);
    let mut i = 2usize;
    let mut topics = Vec::new();
    while i < payload.len() {
        let t = read_utf8(payload, &mut i)?;
        if i >= payload.len() {
            return Err(bad("short SUBSCRIBE qos"));
        }
        topics.push((t, payload[i]));
        i += 1;
    }
    Ok(Packet::Subscribe { packet_id, topics })
}

fn finish(first: u8, variable: Vec<u8>) -> Result<Vec<u8>> {
    if variable.len() > MAX_PACKET_BYTES {
        return Err(ConnectorError::new(
            ErrorCode::MaxRecordSize,
            format!("MQTT packet {}B exceeds {MAX_PACKET_BYTES}", variable.len()),
        ));
    }
    let mut out = vec![first];
    encode_remaining_length(variable.len(), &mut out);
    out.extend_from_slice(&variable);
    Ok(out)
}

pub fn encode_remaining_length(mut len: usize, out: &mut Vec<u8>) {
    if len == 0 {
        out.push(0);
        return;
    }
    loop {
        let mut byte = (len % 128) as u8;
        len /= 128;
        if len > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if len == 0 {
            break;
        }
    }
}

pub fn decode_remaining_length(buf: &[u8]) -> Result<(usize, usize)> {
    let mut mul = 1usize;
    let mut value = 0usize;
    for (i, b) in buf.iter().copied().enumerate() {
        value += (b & 0x7f) as usize * mul;
        mul *= 128;
        if b & 0x80 == 0 {
            if value > MAX_PACKET_BYTES {
                return Err(ConnectorError::new(
                    ErrorCode::MaxRecordSize,
                    format!("MQTT remaining length {value} exceeds {MAX_PACKET_BYTES}"),
                ));
            }
            return Ok((value, i + 1));
        }
        if i >= 3 {
            return Err(bad("MQTT remaining length overflow"));
        }
    }
    Err(bad("incomplete remaining length"))
}

fn write_utf8(out: &mut Vec<u8>, s: &str) {
    write_bytes(out, s.as_bytes());
}

fn write_bytes(out: &mut Vec<u8>, s: &[u8]) {
    let n = s.len() as u16;
    out.extend_from_slice(&n.to_be_bytes());
    out.extend_from_slice(s);
}

fn read_utf8(buf: &[u8], i: &mut usize) -> Result<String> {
    let b = read_bytes(buf, i)?;
    String::from_utf8(b).map_err(|_| bad("MQTT string is not UTF-8"))
}

fn read_bytes(buf: &[u8], i: &mut usize) -> Result<Vec<u8>> {
    if *i + 1 >= buf.len() {
        return Err(bad("short MQTT string"));
    }
    let n = u16::from_be_bytes([buf[*i], buf[*i + 1]]) as usize;
    *i += 2;
    if *i + n > buf.len() {
        return Err(bad("truncated MQTT string"));
    }
    let out = buf[*i..*i + n].to_vec();
    *i += n;
    Ok(out)
}

fn bad(msg: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ErrorCode::CodecViolation, msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_round_trip() {
        let p = Packet::Connect(Connect {
            client_id: "edge-1".into(),
            clean_session: true,
            keepalive: 30,
            username: Some("u".into()),
            password: Some(b"p".to_vec()),
        });
        let bytes = encode(&p).unwrap();
        let (len, hdr) = decode_remaining_length(&bytes[1..]).unwrap();
        let decoded = decode(bytes[0], &bytes[1 + hdr..1 + hdr + len]).unwrap();
        assert_eq!(p, decoded);
    }

    #[test]
    fn publish_qos0_round_trip() {
        let p = Packet::Publish(Publish {
            dup: false,
            qos: 0,
            retain: false,
            topic: "sensors/json".into(),
            packet_id: None,
            payload: br#"{"ok":true}"#.to_vec(),
        });
        let bytes = encode(&p).unwrap();
        let (len, hdr) = decode_remaining_length(&bytes[1..]).unwrap();
        let decoded = decode(bytes[0], &bytes[1 + hdr..1 + hdr + len]).unwrap();
        assert_eq!(p, decoded);
    }
}
