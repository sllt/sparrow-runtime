//! Versioned copy-only protocol; no Rust ABI, host pointers or shared memory.
use serde::{Deserialize, Serialize};
pub const PROTOCOL: &str = "sparrow-extension-ipc-v1";
pub const MAX_FRAME: usize = 65536;
pub const MAX_CONFIG: usize = 4096;
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Source,
    Sink,
    Transform,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Type {
    Bool,
    Int64,
    #[serde(rename = "uint64")]
    UInt64,
    Float64,
    Utf8,
    Bytes,
    TimestampMicrosUtc,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Field {
    pub name: String,
    #[serde(rename = "type")]
    pub kind: Type,
    pub nullable: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    pub max_rows: usize,
    pub max_frame_bytes: usize,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Permission {
    Filesystem,
    Network,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Declaration {
    pub role: Role,
    pub input: Vec<Field>,
    pub output: Vec<Field>,
    pub limits: Limits,
    pub permissions: Vec<Permission>,
    pub watermarks: bool,
}
impl Declaration {
    pub fn validate(&self) -> Result<(), Code> {
        if !(1..=32).contains(&self.limits.max_rows)
            || !(1024..=MAX_FRAME).contains(&self.limits.max_frame_bytes)
            || self.input.len() > 16
            || self.output.len() > 16
            || self.permissions.len() > 2
            || (self.watermarks && self.role != Role::Source)
        {
            return Err(Code::Protocol);
        }
        for fields in [&self.input, &self.output] {
            let mut names = std::collections::BTreeSet::new();
            if fields.iter().any(|f| {
                f.name.is_empty()
                    || f.name.len() > 128
                    || f.name.contains('\0')
                    || !names.insert(&f.name)
            }) {
                return Err(Code::Protocol);
            }
        }
        if self.permissions.len() == 2 && self.permissions[0] == self.permissions[1] {
            return Err(Code::Protocol);
        }
        match self.role {
            Role::Source if self.input.is_empty() && !self.output.is_empty() => Ok(()),
            Role::Sink if !self.input.is_empty() && self.output.is_empty() => Ok(()),
            Role::Transform
                if !self.input.is_empty()
                    && !self.output.is_empty()
                    && self.permissions.is_empty() =>
            {
                Ok(())
            }
            _ => Err(Code::Protocol),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "t", content = "v", deny_unknown_fields)]
pub enum Value {
    Null,
    Bool(bool),
    Int(String),
    UInt(String),
    Float(f64),
    Text(String),
    Bytes(Vec<u8>),
    Time(String),
}
pub type Row = Vec<Value>;
fn bounded<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>, const N: usize>(
    d: D,
) -> Result<Vec<T>, D::Error> {
    struct Array<T, const N: usize>(std::marker::PhantomData<T>);
    impl<'de, T: Deserialize<'de>, const N: usize> serde::de::Visitor<'de> for Array<T, N> {
        type Value = Vec<T>;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "at most {N} elements")
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut seq: A,
        ) -> Result<Self::Value, A::Error> {
            use serde::de::Error;
            let mut result = Vec::new();
            while let Some(value) = seq.next_element()? {
                if result.len() == N {
                    return Err(A::Error::custom("protocol array bound"));
                }
                result.push(value);
            }
            Ok(result)
        }
    }
    d.deserialize_seq(Array::<T, N>(std::marker::PhantomData))
}
fn row<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Row, D::Error> {
    bounded::<D, Value, 16>(d)
}
fn rows<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Vec<Row>, D::Error> {
    #[derive(Deserialize)]
    struct BoundedRow(#[serde(deserialize_with = "row")] Row);
    bounded::<D, BoundedRow, 32>(d).map(|rows| rows.into_iter().map(|r| r.0).collect())
}
pub fn validate_rows(rows: &[Row], schema: &[Field], max_rows: usize) -> Result<(), Code> {
    if rows.len() > max_rows {
        return Err(Code::Limit);
    }
    for row in rows {
        if row.len() != schema.len() {
            return Err(Code::Protocol);
        }
        for (value, field) in row.iter().zip(schema) {
            let valid = match (value, field.kind) {
                (Value::Null, _) => field.nullable,
                (Value::Bool(_), Type::Bool) => true,
                (Value::Int(v), Type::Int64) | (Value::Time(v), Type::TimestampMicrosUtc) => {
                    v.parse::<i64>().is_ok()
                }
                (Value::UInt(v), Type::UInt64) => v.parse::<u64>().is_ok(),
                (Value::Float(v), Type::Float64) => v.is_finite(),
                (Value::Text(v), Type::Utf8) => v.len() <= MAX_FRAME,
                (Value::Bytes(v), Type::Bytes) => v.len() <= MAX_FRAME,
                _ => false,
            };
            if !valid {
                return Err(Code::Protocol);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn protocol_limits_types_nullable_and_exact_integer_roundtrip() {
        let field = Field {
            name: "v".into(),
            kind: Type::UInt64,
            nullable: false,
        };
        let row = vec![Value::UInt(u64::MAX.to_string())];
        validate_rows(std::slice::from_ref(&row), std::slice::from_ref(&field), 1).unwrap();
        assert!(validate_rows(&[vec![Value::Null]], std::slice::from_ref(&field), 1).is_err());
        assert!(validate_rows(
            &[vec![Value::Int("1".into())]],
            std::slice::from_ref(&field),
            1
        )
        .is_err());
        assert!(validate_rows(
            &[vec![Value::UInt("18446744073709551616".into())]],
            &[field],
            1
        )
        .is_err());
        let response = Response {
            sequence: 7,
            reply: Reply::Rows { rows: vec![row] },
        };
        let bytes = encode(&response, MAX_FRAME).unwrap();
        let decoded: Response = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            serde_json::to_value(decoded).unwrap(),
            serde_json::to_value(&response).unwrap()
        );
        assert!(encode(&response, 8).is_err());
    }
    #[test]
    fn untrusted_frame_arrays_are_bounded_during_decode() {
        for rows in [vec![vec![Value::Null]; 33], vec![vec![Value::Null; 17]]] {
            let bytes = serde_json::to_vec(&Response {
                sequence: 1,
                reply: Reply::Rows { rows },
            })
            .unwrap();
            assert!(serde_json::from_slice::<Response>(&bytes).is_err());
        }
        let bytes = serde_json::to_vec(&Request {
            sequence: 1,
            operation: Operation::Transform {
                row: vec![Value::Null; 17],
            },
        })
        .unwrap();
        assert!(serde_json::from_slice::<Request>(&bytes).is_err());
    }
    #[test]
    fn declaration_roles_and_permission_claims_are_strict() {
        let field = Field {
            name: "v".into(),
            kind: Type::Int64,
            nullable: true,
        };
        let mut d = Declaration {
            role: Role::Transform,
            input: vec![field.clone()],
            output: vec![field],
            limits: Limits {
                max_rows: 16,
                max_frame_bytes: 16384,
            },
            permissions: vec![],
            watermarks: false,
        };
        d.validate().unwrap();
        d.permissions.push(Permission::Network);
        assert!(d.validate().is_err());
        d.permissions.clear();
        d.watermarks = true;
        assert!(d.validate().is_err());
        d.watermarks = false;
        d.limits.max_rows = 33;
        assert!(d.validate().is_err());
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Code {
    Protocol,
    InvalidConfig,
    Io,
    Limit,
    Unsupported,
    Failed,
}
impl std::fmt::Display for Code {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Code {}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub sequence: u64,
    pub operation: Operation,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op", deny_unknown_fields)]
pub enum Operation {
    Describe {
        protocol: String,
        role: Role,
    },
    Open {
        config: serde_json::Value,
    },
    Poll {
        max_rows: usize,
    },
    Accepted {
        poll: u64,
    },
    Push {
        #[serde(deserialize_with = "rows")]
        rows: Vec<Row>,
    },
    Transform {
        #[serde(deserialize_with = "row")]
        row: Row,
    },
    Flush,
    Close,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Response {
    pub sequence: u64,
    pub reply: Reply,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", deny_unknown_fields)]
pub enum Reply {
    Description {
        protocol: String,
        declaration: Declaration,
    },
    Opened,
    Data {
        #[serde(deserialize_with = "rows")]
        rows: Vec<Row>,
        watermark: Option<i64>,
    },
    Idle {
        retry_after_ms: u64,
    },
    End,
    Accepted,
    Rows {
        #[serde(deserialize_with = "rows")]
        rows: Vec<Row>,
    },
    Flushed,
    Closed,
    Failure {
        code: Code,
    },
}
pub fn encode<T: Serialize>(value: &T, cap: usize) -> Result<Vec<u8>, Code> {
    use std::io::Write;
    struct Limited {
        data: Vec<u8>,
        cap: usize,
    }
    impl Write for Limited {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.cap.saturating_sub(self.data.len()) {
                return Err(std::io::Error::other("frame limit"));
            }
            self.data.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    if cap > MAX_FRAME {
        return Err(Code::Limit);
    }
    let mut output = Limited {
        data: Vec::with_capacity(cap),
        cap,
    };
    serde_json::to_writer(&mut output, value).map_err(|_| Code::Limit)?;
    Ok(output.data)
}
