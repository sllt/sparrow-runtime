use crate::invalid;
use serde::{Deserialize, Serialize};
use sparrow_model::{DataType, Result};

pub const MAX_ARTIFACT: usize = 4 * 1024 * 1024;
pub const MAX_MANIFEST: usize = 16 * 1024;
pub const MAX_PACKAGES: usize = 16;
pub const MAX_VALUE: usize = 64 * 1024;
pub const JS_TARGET: &str = "javascript-quickjs-ng-0.16.2-v1";
pub const MAX_SCRIPT: usize = 32 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValueType {
    Bool,
    Int64,
    #[serde(rename = "uint64")]
    UInt64,
    Float64,
    Utf8,
    Bytes,
    TimestampMicrosUtc,
}
impl ValueType {
    pub fn data_type(self) -> DataType {
        match self {
            Self::Bool => DataType::Bool,
            Self::Int64 => DataType::Int64,
            Self::UInt64 => DataType::UInt64,
            Self::Float64 => DataType::Float64,
            Self::Utf8 => DataType::Utf8,
            Self::Bytes => DataType::Bytes,
            Self::TimestampMicrosUtc => DataType::TimestampMicrosUTC,
        }
    }
    pub fn tag(self) -> u32 {
        match self {
            Self::Bool => 1,
            Self::Int64 => 2,
            Self::UInt64 => 3,
            Self::Float64 => 4,
            Self::Utf8 => 5,
            Self::Bytes => 6,
            Self::TimestampMicrosUtc => 7,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FunctionDef {
    pub name: String,
    pub id: u32,
    pub inputs: Vec<ValueType>,
    pub output: ValueType,
    pub max_output_bytes: usize,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub format: u32,
    pub name: String,
    pub version: String,
    pub kind: String,
    pub abi: u32,
    pub semantics: u32,
    pub target: String,
    pub artifact_sha256: String,
    pub deterministic: bool,
    pub thread_safe: bool,
    pub null_policy: String,
    pub functions: Vec<FunctionDef>,
}
pub fn identifier(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 32
        && s.bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_')
}
pub fn digest_name(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}
pub fn host_target() -> &'static str {
    if cfg!(all(
        target_os = "linux",
        target_env = "gnu",
        target_endian = "little",
        target_arch = "x86_64"
    )) {
        "x86_64-unknown-linux-gnu"
    } else if cfg!(all(
        target_os = "linux",
        target_env = "gnu",
        target_endian = "little",
        target_arch = "aarch64"
    )) {
        "aarch64-unknown-linux-gnu"
    } else {
        "unsupported"
    }
}
pub fn sha256(bytes: &[u8]) -> String {
    ring::digest::digest(&ring::digest::SHA256, bytes)
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
impl Manifest {
    pub fn is_script(&self) -> bool {
        self.kind == "javascript_scalar"
    }
    pub fn artifact_name(&self) -> &'static str {
        if self.is_script() {
            "artifact.js"
        } else {
            "artifact.so"
        }
    }
    pub fn validate(&self) -> Result<()> {
        let target_ok = if self.is_script() {
            self.target == JS_TARGET && host_target() != "unsupported"
        } else {
            self.kind == "native_scalar"
                && self.target == host_target()
                && host_target() != "unsupported"
        };
        if self.format != 1
            || self.abi != 1
            || self.semantics != 1
            || !target_ok
            || !identifier(&self.name)
            || !identifier(&self.version)
            || !digest_name(&self.artifact_sha256)
            || !self.deterministic
            || !self.thread_safe
            || self.null_policy != "propagate"
            || !(1..=16).contains(&self.functions.len())
        {
            return Err(invalid(
                "unsupported plugin manifest/version/target or scalar trust contract",
            ));
        }
        let mut names = std::collections::BTreeSet::new();
        let mut ids = std::collections::BTreeSet::new();
        for f in &self.functions {
            if !identifier(&f.name)
                || f.id == 0
                || !names.insert(&f.name)
                || !ids.insert(f.id)
                || f.inputs.len() > 8
                || !(1..=MAX_VALUE).contains(&f.max_output_bytes)
            {
                return Err(invalid(
                    "invalid/duplicate plugin function or resource declaration",
                ));
            }
        }
        if serde_json::to_vec(self)
            .map_err(|_| invalid("invalid manifest"))?
            .len()
            > MAX_MANIFEST
        {
            return Err(invalid("plugin manifest too large"));
        }
        Ok(())
    }
    pub fn bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        serde_json::to_vec(self).map_err(|_| invalid("invalid manifest"))
    }
    pub fn identity(&self) -> Result<String> {
        Ok(sha256(&self.bytes()?))
    }
    pub fn check_artifact(&self, bytes: &[u8]) -> Result<()> {
        self.validate()?;
        if bytes.is_empty() || bytes.len() > MAX_ARTIFACT || sha256(bytes) != self.artifact_sha256 {
            return Err(invalid("plugin artifact size/hash mismatch"));
        }
        if self.is_script() {
            if bytes.len() > MAX_SCRIPT || std::str::from_utf8(bytes).is_err() {
                return Err(invalid("JavaScript source must be UTF-8 and at most 32KiB"));
            }
            return Ok(());
        }
        // Reject a wrong class/endian/architecture before executing any loader code.
        let machine = if cfg!(target_arch = "x86_64") {
            62u16
        } else {
            183
        };
        if bytes.len() < 20
            || &bytes[..4] != b"\x7fELF"
            || bytes[4] != 2
            || bytes[5] != 1
            || u16::from_le_bytes([bytes[16], bytes[17]]) != 3
            || u16::from_le_bytes([bytes[18], bytes[19]]) != machine
        {
            return Err(invalid(
                "plugin requires a matching ELF64 little-endian shared object",
            ));
        }
        Ok(())
    }
}
