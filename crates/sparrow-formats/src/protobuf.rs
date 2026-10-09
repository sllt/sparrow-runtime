//! Bounded protobuf <-> [`Row`] codec driven by a user-supplied
//! `FileDescriptorSet` (`protoc --include_imports --descriptor_set_out`).
//!
//! `prost-reflect` (`=0.16.5`) decodes and validates the descriptor set and
//! resolves the message type, field paths, presence and enums. Messages are
//! *not* decoded into a `DynamicMessage`: that tree costs a B-tree node per
//! set field and per nested message (well over 100 bytes per wire byte for
//! small nested messages), which a 64 KiB message cannot afford under a 4 MiB
//! job reservation. Instead one validating pass walks the wire bytes once:
//!
//! - every key, varint, length and group is checked (truncation, wire types
//!   6/7, field number 0, unbalanced groups are malformed);
//! - every *declared* field must use its declared wire type (repeated
//!   packable scalars accept both packed and unpacked), every declared
//!   `string` must be UTF-8 and every declared message, group and map entry
//!   is descended into, so `max_depth` bounds the whole message, not just the
//!   mapped part;
//! - undeclared fields follow `unknown_fields` (`ignore` skips them; an
//!   unknown length-delimited field is opaque and not descended into, an
//!   unknown group is skipped structurally and counts towards the depth);
//! - mapped fields are recorded as borrowed raw values with protobuf merge
//!   semantics (last scalar wins, a repeated singular message merges, a oneof
//!   member clears the other members) and converted once at the end.
//!
//! The decoder allocates only the output row and its strings/bytes (bounded
//! by the wire length) plus per-column state; see
//! [`ProtobufFormat::decode_scratch`].
//!
//! Encoding writes fields in field-number order with proto3 implicit-presence
//! defaults omitted, the same bytes `protoc --encode` produces for the same
//! values (golden tests).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use base64::Engine;
use prost::encoding::{
    decode_varint, encode_key, encode_varint, encoded_len_varint, key_len, WireType,
};
use prost_reflect::{Cardinality, DescriptorPool, EnumDescriptor, Kind, MessageDescriptor, Syntax};
use serde::{Deserialize, Serialize};
use sparrow_model::{
    DataType, ErrorCode, Field, MemoryOwner, Result, Row, Scalar, Schema, SparrowError,
};

use crate::csv::CsvRole as FormatRole;

/// Upper bound for one message (and the default), matching the JSON / CSV
/// record cap of every byte-oriented connector.
pub const MAX_PROTOBUF_MESSAGE_BYTES: usize = 64 * 1024;
/// Upper bound for `max_depth`: protobuf's own default recursion limit.
pub const MAX_PROTOBUF_DEPTH: usize = 100;
/// Decoded `FileDescriptorSet` bytes. The whole pipeline spec is <=64 KiB.
pub const MAX_DESCRIPTOR_SET_BYTES: usize = 48 * 1024;
const DEFAULT_MAX_DEPTH: usize = 32;
const MAX_PATH_BYTES: usize = 512;
const MAX_COLUMN_NAME: usize = 256;
const MAX_PLAN_FIELDS: usize = 64;
const MAX_PLAN_NODES: usize = 256;
const MAX_DESCRIPTOR_NAME: usize = 256;
const MAX_QUALIFIED_NAME: usize = 1024;
const TIMESTAMP: &str = "google.protobuf.Timestamp";
/// 0001-01-01T00:00:00Z and 9999-12-31T23:59:59Z, the `Timestamp` range.
const MIN_TIMESTAMP_SECONDS: i64 = -62_135_596_800;
const MAX_TIMESTAMP_SECONDS: i64 = 253_402_300_799;

fn err(code: ErrorCode, message: impl Into<String>) -> SparrowError {
    SparrowError::new(code, message)
}

fn malformed(message: impl Into<String>) -> SparrowError {
    err(ErrorCode::CodecViolation, message)
}

fn is_default<T: Default + PartialEq>(value: &T) -> bool {
    *value == T::default()
}

/// Fields the descriptor does not declare.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnknownFields {
    /// Skip them (protobuf's forward-compatible default).
    #[default]
    Ignore,
    /// Refuse the message (`invalid_schema`, counted as unknown-field).
    Error,
}

/// User-facing protobuf options (`source.protobuf` / `sink.protobuf`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProtobufOptions {
    /// Standard (padded) base64 of a serialized `FileDescriptorSet` that
    /// contains the message type and all its imports.
    pub descriptor_set: String,
    /// Fully qualified message name, e.g. `telemetry.v1.Reading`.
    pub message: String,
    /// Schema field -> dotted protobuf field path (`location.lat`). A schema
    /// field without an entry maps to the top-level field of the same name.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub fields: BTreeMap<String, String>,
    /// Decode only.
    #[serde(default, skip_serializing_if = "is_default")]
    pub unknown_fields: UnknownFields,
    /// Decode only: bytes per message, 1..=65536 (default 65536).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_message_bytes: Option<usize>,
    /// Decode only: message nesting depth (top level = 1), 1..=100
    /// (default 32). Bounds every declared nested message, group and map
    /// entry, and unknown groups.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_depth: Option<usize>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProtobufLimits {
    pub max_message_bytes: usize,
    pub max_depth: usize,
}

impl ProtobufOptions {
    /// Validate for one role and compile: decode and resolve the descriptor
    /// set and the message type. Decode-only options on a sink are refused.
    /// The schema mapping is checked by [`ProtobufFormat::check_schema`].
    pub fn compile(&self, role: FormatRole) -> Result<ProtobufFormat> {
        if self.message.is_empty()
            || self.message.len() > MAX_QUALIFIED_NAME
            || self.fields.len() > MAX_PLAN_FIELDS
        {
            return Err(err(
                ErrorCode::BoundExceeded,
                "protobuf message name or mapping count exceeds its bound",
            ));
        }
        if role == FormatRole::Encode {
            let decode_only = [
                (
                    "unknown_fields",
                    self.unknown_fields != UnknownFields::default(),
                ),
                ("max_message_bytes", self.max_message_bytes.is_some()),
                ("max_depth", self.max_depth.is_some()),
            ];
            if let Some((name, _)) = decode_only.iter().find(|(_, set)| *set) {
                return Err(err(
                    ErrorCode::InvalidArgument,
                    format!("protobuf.{name} is a decode option and is not accepted on a sink"),
                ));
            }
        }
        let max_message_bytes = self.max_message_bytes.unwrap_or(MAX_PROTOBUF_MESSAGE_BYTES);
        if !(1..=MAX_PROTOBUF_MESSAGE_BYTES).contains(&max_message_bytes) {
            return Err(err(
                ErrorCode::BoundExceeded,
                format!("protobuf.max_message_bytes must be 1..={MAX_PROTOBUF_MESSAGE_BYTES}"),
            ));
        }
        let max_depth = self.max_depth.unwrap_or(DEFAULT_MAX_DEPTH);
        if !(1..=MAX_PROTOBUF_DEPTH).contains(&max_depth) {
            return Err(err(
                ErrorCode::BoundExceeded,
                format!("protobuf.max_depth must be 1..={MAX_PROTOBUF_DEPTH}"),
            ));
        }
        // Reject on length before decoding: base64 is 4 chars per 3 bytes.
        if self.descriptor_set.len() > MAX_DESCRIPTOR_SET_BYTES.div_ceil(3) * 4 {
            return Err(err(
                ErrorCode::BoundExceeded,
                format!("protobuf.descriptor_set exceeds {MAX_DESCRIPTOR_SET_BYTES} bytes"),
            ));
        }
        let descriptor = base64::engine::general_purpose::STANDARD
            .decode(self.descriptor_set.as_bytes())
            .map_err(|_| {
                err(
                    ErrorCode::InvalidArgument,
                    "protobuf.descriptor_set must be standard padded base64",
                )
            })?;
        if descriptor.is_empty() || descriptor.len() > MAX_DESCRIPTOR_SET_BYTES {
            return Err(err(
                ErrorCode::BoundExceeded,
                format!(
                    "protobuf.descriptor_set must decode to 1..={MAX_DESCRIPTOR_SET_BYTES} bytes"
                ),
            ));
        }
        let pool = build_pool(&descriptor)?;
        let message = pool.get_message_by_name(&self.message).ok_or_else(|| {
            err(
                ErrorCode::InvalidSchema,
                format!(
                    "protobuf.message `{}` is not in the descriptor set",
                    self.message
                ),
            )
        })?;
        if message.is_map_entry() {
            return Err(err(
                ErrorCode::InvalidSchema,
                "protobuf.message must not be a synthetic map entry",
            ));
        }
        for (column, path) in &self.fields {
            if column.is_empty() || column.len() > MAX_COLUMN_NAME {
                return Err(err(
                    ErrorCode::InvalidArgument,
                    format!("protobuf.fields column names must be 1..={MAX_COLUMN_NAME} bytes"),
                ));
            }
            check_path(path)?;
        }
        Ok(ProtobufFormat {
            options: self.clone(),
            role,
            descriptor,
            message,
            limits: ProtobufLimits {
                max_message_bytes,
                max_depth,
            },
            plan: Mutex::new(None),
        })
    }
}

/// Decode and link the descriptor set.
///
/// `syntax` is checked before linking: prost-reflect 0.16.5 refuses anything
/// but proto2/proto3 (so editions files are refused), but its error label for
/// that case indexes the file table before the file is added and panics
/// (`build/names.rs` `visit_file` -> `Label::new`). The pre-check keeps user
/// input off that path; `catch_unwind` is a second line for any other panic
/// while linking untrusted descriptors.
fn build_pool(descriptor: &[u8]) -> Result<DescriptorPool> {
    use prost::Message as _;
    let invalid = |detail: String| {
        err(
            ErrorCode::InvalidSchema,
            format!("protobuf.descriptor_set is not a valid FileDescriptorSet: {detail}"),
        )
    };
    let set = prost_reflect::prost_types::FileDescriptorSet::decode(descriptor)
        .map_err(|e| invalid(e.to_string()))?;
    check_descriptor_complexity(&set)?;
    for file in &set.file {
        match file.syntax.as_deref() {
            None | Some("proto2") | Some("proto3") => {}
            Some(other) => {
                return Err(err(
                    ErrorCode::InvalidSchema,
                    format!(
                        "protobuf.descriptor_set file `{}` has syntax `{other}`; only proto2 and proto3 are supported (not editions)",
                        file.name()
                    ),
                ))
            }
        }
    }
    let pool = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        DescriptorPool::from_file_descriptor_set(set)
    }))
    .map_err(|_| invalid("descriptor linking failed".into()))?
    .map_err(|e| invalid(e.to_string()))?;
    // The Timestamp fast path relies on the canonical WKT shape, not on
    // an untrusted descriptor merely claiming the well-known name.
    if let Some(m) = pool.get_message_by_name(TIMESTAMP) {
        let canonical = m.parent_file().syntax() == Syntax::Proto3
            && m.fields().len() == 2
            && [(1, "seconds", Kind::Int64), (2, "nanos", Kind::Int32)]
                .iter()
                .all(|(n, name, kind)| {
                    m.get_field(*n).is_some_and(|f| {
                        f.name() == *name
                            && f.kind() == *kind
                            && f.cardinality() == Cardinality::Optional
                            && !f.supports_presence()
                            && f.containing_oneof().is_none()
                            && !f.is_group()
                    })
                });
        if !canonical {
            return Err(invalid(
                "google.protobuf.Timestamp must have its canonical proto3 seconds/nanos shape"
                    .into(),
            ));
        }
    }
    Ok(pool)
}

/// Bound descriptor expansion BEFORE prost-reflect constructs qualified
/// names and lookup tables. A small wire descriptor can otherwise expand
/// a huge package prefix once per symbol. Configuration metadata is bounded
/// separately from per-record scratch.
fn check_descriptor_complexity(set: &prost_reflect::prost_types::FileDescriptorSet) -> Result<()> {
    use prost_reflect::prost_types::{DescriptorProto, EnumDescriptorProto, FieldDescriptorProto};
    struct Budget {
        symbols: usize,
        names: usize,
    }
    impl Budget {
        fn name(&mut self, name: &str, prefix: usize) -> Result<usize> {
            let qualified = prefix.saturating_add(name.len()).saturating_add(1);
            self.symbols = self.symbols.saturating_add(1);
            self.names = self.names.saturating_add(qualified);
            if name.len() > MAX_DESCRIPTOR_NAME
                || qualified > MAX_QUALIFIED_NAME
                || self.symbols > 2048
                || self.names > 512 * 1024
            {
                return Err(err(
                    ErrorCode::BoundExceeded,
                    "protobuf descriptor symbol/name expansion exceeds its bound",
                ));
            }
            Ok(qualified)
        }
        fn field(&mut self, f: &FieldDescriptorProto, prefix: usize) -> Result<()> {
            self.name(f.name(), prefix)?;
            if f.type_name().len() > MAX_QUALIFIED_NAME
                || f.extendee().len() > MAX_QUALIFIED_NAME
                || f.json_name().len() > MAX_DESCRIPTOR_NAME
            {
                return Err(err(
                    ErrorCode::BoundExceeded,
                    "protobuf descriptor field name exceeds its bound",
                ));
            }
            Ok(())
        }
        fn enumeration(&mut self, e: &EnumDescriptorProto, prefix: usize) -> Result<()> {
            let scope = self.name(e.name(), prefix)?;
            for v in &e.value {
                self.name(v.name(), scope)?;
            }
            Ok(())
        }
        fn message(&mut self, m: &DescriptorProto, prefix: usize, depth: usize) -> Result<()> {
            if depth > 32 {
                return Err(err(
                    ErrorCode::BoundExceeded,
                    "protobuf descriptor nesting exceeds 32",
                ));
            }
            let scope = self.name(m.name(), prefix)?;
            for f in m.field.iter().chain(&m.extension) {
                self.field(f, scope)?;
            }
            for o in &m.oneof_decl {
                self.name(o.name(), scope)?;
            }
            for e in &m.enum_type {
                self.enumeration(e, scope)?;
            }
            for n in &m.nested_type {
                self.message(n, scope, depth + 1)?;
            }
            Ok(())
        }
    }
    if set.file.len() > 32 {
        return Err(err(
            ErrorCode::BoundExceeded,
            "protobuf descriptor contains more than 32 files",
        ));
    }
    let mut budget = Budget {
        symbols: 0,
        names: 0,
    };
    for f in &set.file {
        budget.name(f.name(), 0)?;
        let scope = budget.name(f.package(), 0)?;
        for m in &f.message_type {
            budget.message(m, scope, 1)?;
        }
        for e in &f.enum_type {
            budget.enumeration(e, scope)?;
        }
        for e in &f.extension {
            budget.field(e, scope)?;
        }
        for s in &f.service {
            let scope = budget.name(s.name(), scope)?;
            for m in &s.method {
                budget.name(m.name(), scope)?;
                if m.input_type().len() > MAX_QUALIFIED_NAME
                    || m.output_type().len() > MAX_QUALIFIED_NAME
                {
                    return Err(err(
                        ErrorCode::BoundExceeded,
                        "protobuf method type name exceeds its bound",
                    ));
                }
            }
        }
    }
    Ok(())
}

fn check_path(path: &str) -> Result<()> {
    let ok = !path.is_empty()
        && path.len() <= MAX_PATH_BYTES
        && path.split('.').all(|segment| {
            !segment.is_empty()
                && segment
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_')
        });
    if ok {
        Ok(())
    } else {
        Err(err(
            ErrorCode::InvalidArgument,
            format!(
                "protobuf field path `{path}` must be dot-separated field names (<= {MAX_PATH_BYTES} bytes)"
            ),
        ))
    }
}

/// Validated descriptor, message type, mapping options and limits.
pub struct ProtobufFormat {
    options: ProtobufOptions,
    role: FormatRole,
    descriptor: Vec<u8>,
    message: MessageDescriptor,
    limits: ProtobufLimits,
    /// Compiled mapping for the last schema seen (connectors use one).
    plan: Mutex<Option<(Schema, Arc<Plan>)>>,
}

impl Clone for ProtobufFormat {
    fn clone(&self) -> Self {
        Self {
            options: self.options.clone(),
            role: self.role,
            descriptor: self.descriptor.clone(),
            message: self.message.clone(),
            limits: self.limits,
            plan: Mutex::new(None),
        }
    }
}

impl PartialEq for ProtobufFormat {
    fn eq(&self, other: &Self) -> bool {
        self.options == other.options && self.role == other.role
    }
}

impl Eq for ProtobufFormat {}

impl std::fmt::Debug for ProtobufFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProtobufFormat")
            .field("message", &self.options.message)
            .field("descriptor_bytes", &self.descriptor.len())
            .field("fields", &self.options.fields)
            .field("unknown_fields", &self.options.unknown_fields)
            .field("limits", &self.limits)
            .finish()
    }
}

/// Classification used by connectors for the separate protobuf counters.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProtobufFault {
    /// Message length or nesting depth over a limit.
    Oversize,
    /// Wire structure: truncation, bad varint/key, wire-type mismatch,
    /// invalid UTF-8 in a declared string, unbalanced group.
    Malformed,
    /// An undeclared field under `unknown_fields: error`.
    UnknownField,
    /// A value that does not fit its column (NULL where required, range,
    /// unknown enum name/number, sub-microsecond timestamp, non-finite float).
    Type,
}

impl ProtobufFault {
    pub fn of(error: &SparrowError) -> Self {
        match error.code {
            ErrorCode::MaxRecordSize | ErrorCode::BoundExceeded => Self::Oversize,
            ErrorCode::CodecViolation => Self::Malformed,
            ErrorCode::InvalidSchema => Self::UnknownField,
            _ => Self::Type,
        }
    }
}

// ---------------------------------------------------------------------------
// Plan: schema columns -> field paths.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScalarKind {
    Double,
    Float,
    Int32,
    Int64,
    Uint32,
    Uint64,
    Sint32,
    Sint64,
    Fixed32,
    Fixed64,
    Sfixed32,
    Sfixed64,
    Bool,
    String,
    Bytes,
}

impl ScalarKind {
    fn wire(self) -> WireType {
        match self {
            Self::Double | Self::Fixed64 | Self::Sfixed64 => WireType::SixtyFourBit,
            Self::Float | Self::Fixed32 | Self::Sfixed32 => WireType::ThirtyTwoBit,
            Self::String | Self::Bytes => WireType::LengthDelimited,
            _ => WireType::Varint,
        }
    }

    fn of(kind: &Kind) -> Option<Self> {
        Some(match kind {
            Kind::Double => Self::Double,
            Kind::Float => Self::Float,
            Kind::Int32 => Self::Int32,
            Kind::Int64 => Self::Int64,
            Kind::Uint32 => Self::Uint32,
            Kind::Uint64 => Self::Uint64,
            Kind::Sint32 => Self::Sint32,
            Kind::Sint64 => Self::Sint64,
            Kind::Fixed32 => Self::Fixed32,
            Kind::Fixed64 => Self::Fixed64,
            Kind::Sfixed32 => Self::Sfixed32,
            Kind::Sfixed64 => Self::Sfixed64,
            Kind::Bool => Self::Bool,
            Kind::String => Self::String,
            Kind::Bytes => Self::Bytes,
            Kind::Message(_) | Kind::Enum(_) => return None,
        })
    }
}

#[derive(Clone, Debug)]
enum LeafKind {
    Scalar(ScalarKind),
    /// `closed`: a proto2 enum, whose undeclared numbers are refused.
    Enum {
        desc: EnumDescriptor,
        closed: bool,
    },
    /// `google.protobuf.Timestamp` <-> TimestampMicrosUTC.
    Timestamp,
}

impl LeafKind {
    fn wire(&self) -> WireType {
        match self {
            Self::Scalar(kind) => kind.wire(),
            Self::Enum { .. } => WireType::Varint,
            Self::Timestamp => WireType::LengthDelimited,
        }
    }

    fn fits(&self, ty: &DataType) -> bool {
        use ScalarKind as S;
        match self {
            Self::Scalar(S::Int32 | S::Sint32 | S::Sfixed32) => *ty == DataType::Int64,
            Self::Scalar(S::Int64 | S::Sint64 | S::Sfixed64) => {
                matches!(ty, DataType::Int64 | DataType::TimestampMicrosUTC)
            }
            Self::Scalar(S::Uint32 | S::Fixed32) => {
                matches!(ty, DataType::Int64 | DataType::UInt64)
            }
            Self::Scalar(S::Uint64 | S::Fixed64) => *ty == DataType::UInt64,
            Self::Scalar(S::Float | S::Double) => *ty == DataType::Float64,
            Self::Scalar(S::Bool) => *ty == DataType::Bool,
            Self::Scalar(S::String) => *ty == DataType::Utf8,
            Self::Scalar(S::Bytes) => *ty == DataType::Bytes,
            Self::Enum { .. } => matches!(ty, DataType::Int64 | DataType::Utf8),
            Self::Timestamp => *ty == DataType::TimestampMicrosUTC,
        }
    }

    fn name(&self) -> String {
        match self {
            Self::Scalar(kind) => format!("{kind:?}").to_ascii_lowercase(),
            Self::Enum { desc, .. } => format!("enum {}", desc.full_name()),
            Self::Timestamp => TIMESTAMP.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Slot {
    Leaf(usize),
    Node(usize),
}

#[derive(Debug)]
struct Node {
    desc: MessageDescriptor,
    parent: Option<usize>,
    /// Mapped fields of this message, sorted by field number.
    slots: Vec<(u32, Slot)>,
    /// One list per mapped oneof, NOT one duplicated list per descriptor
    /// member (which expands quadratically for wide oneofs).
    oneofs: Vec<(String, Vec<(u32, Slot)>)>,
}

impl Node {
    fn new(desc: MessageDescriptor, parent: Option<usize>) -> Self {
        Self {
            desc,
            parent,
            slots: Vec::new(),
            oneofs: Vec::new(),
        }
    }
}

#[derive(Debug)]
struct Leaf {
    node: usize,
    kind: LeafKind,
    presence: bool,
    path: String,
    column: String,
}

#[derive(Debug)]
struct Plan {
    nodes: Vec<Node>,
    leaves: Vec<Leaf>,
}

impl Plan {
    fn slot(&self, node: usize, number: u32) -> Option<Slot> {
        let slots = &self.nodes[node].slots;
        slots
            .binary_search_by_key(&number, |(n, _)| *n)
            .ok()
            .map(|i| slots[i].1)
    }
}

impl ProtobufFormat {
    pub fn options(&self) -> &ProtobufOptions {
        &self.options
    }

    pub fn limits(&self) -> ProtobufLimits {
        self.limits
    }

    pub fn message_name(&self) -> &str {
        self.message.full_name()
    }

    /// Resolve the mapping of `schema` (also run before a job starts).
    pub fn check_schema(&self, schema: &Schema) -> Result<()> {
        self.plan(schema).map(|_| ())
    }

    fn plan(&self, schema: &Schema) -> Result<Arc<Plan>> {
        let mut cached = self.plan.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((known, plan)) = cached.as_ref() {
            if known == schema {
                return Ok(plan.clone());
            }
        }
        let plan = Arc::new(self.build_plan(schema)?);
        *cached = Some((schema.clone(), plan.clone()));
        Ok(plan)
    }

    fn build_plan(&self, schema: &Schema) -> Result<Plan> {
        if schema.fields.len() > MAX_PLAN_FIELDS
            || schema.fields.iter().any(|f| f.name.len() > MAX_COLUMN_NAME)
        {
            return Err(err(
                ErrorCode::BoundExceeded,
                "protobuf maps at most 64 fields with names <=256 bytes",
            ));
        }
        let invalid = |message: String| err(ErrorCode::InvalidSchema, message);
        for column in self.options.fields.keys() {
            if !schema.fields.iter().any(|f| &f.name == column) {
                return Err(invalid(format!(
                    "protobuf.fields maps `{column}`, which is not a schema field"
                )));
            }
        }
        let depth_limit = match self.role {
            FormatRole::Decode => self.limits.max_depth,
            FormatRole::Encode => MAX_PROTOBUF_DEPTH,
        };
        let mut nodes = vec![Node::new(self.message.clone(), None)];
        let mut leaves = Vec::with_capacity(schema.fields.len());
        for (index, field) in schema.fields.iter().enumerate() {
            if field.name.contains('.') && !self.options.fields.contains_key(&field.name) {
                return Err(invalid(
                    "a dotted schema column requires an explicit protobuf.fields path".into(),
                ));
            }
            let path = self
                .options
                .fields
                .get(&field.name)
                .cloned()
                .unwrap_or_else(|| field.name.clone());
            check_path(&path)?;
            let segments: Vec<&str> = path.split('.').collect();
            let (last, parents) = segments.split_last().expect("non-empty path");
            let mut node = 0usize;
            let mut depth = 1usize;
            let lookup = |nodes: &[Node], node: usize, segment: &str| {
                let desc = &nodes[node].desc;
                let proto = desc.get_field_by_name(segment).ok_or_else(|| {
                    invalid(format!(
                        "column '{}': `{}` has no field `{segment}` (path `{path}`)",
                        field.name,
                        desc.full_name()
                    ))
                })?;
                if proto.cardinality() == Cardinality::Repeated || proto.is_map() {
                    return Err(invalid(format!(
                        "column '{}': `{segment}` in `{path}` is repeated or a map; those fields are not mapped",
                        field.name
                    )));
                }
                if proto.is_group() {
                    return Err(invalid(format!(
                        "column '{}': `{segment}` in `{path}` is a group; groups are not mapped",
                        field.name
                    )));
                }
                let existing = nodes[node]
                    .slots
                    .iter()
                    .find(|(n, _)| *n == proto.number())
                    .map(|(_, s)| *s);
                Ok((proto, existing))
            };
            for segment in parents {
                let (proto, existing) = lookup(&nodes, node, segment)?;
                let child = match proto.kind() {
                    Kind::Message(child) if child.full_name() != TIMESTAMP => child,
                    _ => {
                        return Err(invalid(format!(
                            "column '{}': `{segment}` in `{path}` is not a message that can hold a path",
                            field.name
                        )))
                    }
                };
                depth += 1;
                node = match existing {
                    Some(Slot::Node(j)) => j,
                    Some(Slot::Leaf(_)) => {
                        return Err(invalid(format!(
                            "column '{}': `{path}` overlaps another column's path",
                            field.name
                        )))
                    }
                    None => {
                        if nodes.len() == MAX_PLAN_NODES {
                            return Err(err(
                                ErrorCode::BoundExceeded,
                                "protobuf mapping exceeds 256 message nodes",
                            ));
                        }
                        let j = nodes.len();
                        nodes.push(Node::new(child, Some(node)));
                        nodes[node].slots.push((proto.number(), Slot::Node(j)));
                        j
                    }
                };
            }
            let (proto, existing) = lookup(&nodes, node, last)?;
            if existing.is_some() {
                return Err(invalid(format!(
                    "column '{}': `{path}` is mapped twice or overlaps another column's path",
                    field.name
                )));
            }
            let kind = match proto.kind() {
                Kind::Message(m) if m.full_name() == TIMESTAMP => {
                    depth += 1;
                    LeafKind::Timestamp
                }
                Kind::Message(m) => {
                    return Err(invalid(format!(
                    "column '{}': `{path}` is message `{}`; map its scalar fields by path instead",
                    field.name,
                    m.full_name()
                )))
                }
                Kind::Enum(e) => LeafKind::Enum {
                    closed: e.parent_file().syntax() == Syntax::Proto2,
                    desc: e,
                },
                other => LeafKind::Scalar(ScalarKind::of(&other).expect("scalar kind")),
            };
            if !kind.fits(&field.data_type) {
                return Err(invalid(format!(
                    "column '{}' ({:?}) cannot map protobuf {} `{path}`",
                    field.name,
                    field.data_type,
                    kind.name()
                )));
            }
            if depth > depth_limit {
                return Err(err(
                    ErrorCode::BoundExceeded,
                    format!(
                        "column '{}': `{path}` nests {depth} messages, over the depth limit {depth_limit}",
                        field.name
                    ),
                ));
            }
            let presence = proto.supports_presence();
            if self.role == FormatRole::Encode && field.nullable && !presence {
                return Err(invalid(format!(
                    "column '{}' is nullable but `{path}` has no presence (proto3 implicit): NULL would be written as the default value; make the column non-nullable or the field `optional`",
                    field.name
                )));
            }
            nodes[node].slots.push((proto.number(), Slot::Leaf(index)));
            leaves.push(Leaf {
                node,
                kind,
                presence,
                path,
                column: field.name.clone(),
            });
        }
        for node in &mut nodes {
            node.slots.sort_by_key(|(n, _)| *n);
        }
        // Oneofs: setting any member clears the mapped other members.
        for node in &mut nodes {
            let mut groups = Vec::new();
            for oneof in node.desc.oneofs() {
                let mapped: Vec<(u32, Slot)> = oneof
                    .fields()
                    .filter_map(|f| node.slots.iter().find(|(n, _)| *n == f.number()).copied())
                    .collect();
                if mapped.is_empty() {
                    continue;
                }
                groups.push((oneof.full_name().to_string(), mapped));
            }
            node.oneofs = groups;
        }
        if self.role == FormatRole::Encode {
            check_required(&nodes, schema)?;
        }
        Ok(Plan { nodes, leaves })
    }
}

/// proto2 `required` fields of every message the encoder may write must be
/// written whenever that message is: a mapped non-nullable leaf, or a mapped
/// message with at least one non-nullable leaf below it.
fn check_required(nodes: &[Node], schema: &Schema) -> Result<()> {
    let mut nonnull = vec![false; nodes.len()];
    for j in (0..nodes.len()).rev() {
        nonnull[j] = nodes[j].slots.iter().any(|(_, slot)| match *slot {
            Slot::Leaf(i) => !schema.fields[i].nullable,
            Slot::Node(k) => nonnull[k],
        });
    }
    let always = |slot: Slot| match slot {
        Slot::Leaf(i) => !schema.fields[i].nullable,
        Slot::Node(j) => nonnull[j],
    };
    for node in nodes {
        for field in node.desc.fields().filter(|f| f.is_required()) {
            let slot = node
                .slots
                .iter()
                .find(|(n, _)| *n == field.number())
                .map(|(_, s)| *s);
            if !slot.is_some_and(always) {
                return Err(err(
                    ErrorCode::InvalidSchema,
                    format!(
                        "proto2 required field `{}` must be mapped to a non-nullable column (or a message path with one)",
                        field.full_name()
                    ),
                ));
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Decode.

#[derive(Clone, Copy, Debug)]
enum Raw<'a> {
    Varint(u64),
    Fixed32(u32),
    Fixed64(u64),
    Len(&'a [u8]),
    Time { seconds: i64, nanos: i32 },
}

/// What a declared field (or extension) looks like on the wire.
struct FieldInfo {
    kind: Kind,
    list: bool,
    map: bool,
    group: bool,
}

fn field_info(desc: &MessageDescriptor, number: u32) -> Option<FieldInfo> {
    if let Some(f) = desc.get_field(number) {
        return Some(FieldInfo {
            kind: f.kind(),
            list: f.is_list(),
            map: f.is_map(),
            group: f.is_group(),
        });
    }
    desc.get_extension(number).map(|f| FieldInfo {
        kind: f.kind(),
        list: f.is_list(),
        map: f.is_map(),
        group: f.is_group(),
    })
}

struct Walker<'p, 'a> {
    plan: &'p Plan,
    bytes: &'a [u8],
    unknown: UnknownFields,
    max_depth: usize,
    leaves: Vec<Option<Raw<'a>>>,
    present: Vec<bool>,
}

impl<'a> Walker<'_, 'a> {
    fn varint(&self, pos: &mut usize, end: usize) -> Result<u64> {
        let mut rest = &self.bytes[*pos..end];
        let value = decode_varint(&mut rest)
            .map_err(|_| malformed("protobuf: truncated or invalid varint"))?;
        *pos = end - rest.len();
        Ok(value)
    }

    fn take(&self, pos: &mut usize, end: usize, len: u64) -> Result<(usize, usize)> {
        let len = usize::try_from(len)
            .ok()
            .filter(|len| *len <= end - *pos)
            .ok_or_else(|| malformed("protobuf: truncated field"))?;
        let start = *pos;
        *pos += len;
        Ok((start, start + len))
    }

    fn key(&self, pos: &mut usize, end: usize) -> Result<(u32, WireType)> {
        let key = self.varint(pos, end)?;
        let wire = match key & 7 {
            0 => WireType::Varint,
            1 => WireType::SixtyFourBit,
            2 => WireType::LengthDelimited,
            3 => WireType::StartGroup,
            4 => WireType::EndGroup,
            5 => WireType::ThirtyTwoBit,
            other => return Err(malformed(format!("protobuf: invalid wire type {other}"))),
        };
        let number = key >> 3;
        if number == 0 || number > (1 << 29) - 1 {
            return Err(malformed(format!(
                "protobuf: invalid field number {number}"
            )));
        }
        Ok((number as u32, wire))
    }

    fn deeper(&self, depth: usize) -> Result<usize> {
        let depth = depth + 1;
        if depth > self.max_depth {
            return Err(err(
                ErrorCode::BoundExceeded,
                format!(
                    "protobuf message nesting exceeds max_depth {}",
                    self.max_depth
                ),
            ));
        }
        Ok(depth)
    }

    /// One non-group value of `wire`.
    fn value(&self, pos: &mut usize, end: usize, wire: WireType) -> Result<Raw<'a>> {
        let bytes: &'a [u8] = self.bytes;
        Ok(match wire {
            WireType::Varint => Raw::Varint(self.varint(pos, end)?),
            WireType::SixtyFourBit => {
                let (s, e) = self.take(pos, end, 8)?;
                Raw::Fixed64(u64::from_le_bytes(bytes[s..e].try_into().expect("8 bytes")))
            }
            WireType::ThirtyTwoBit => {
                let (s, e) = self.take(pos, end, 4)?;
                Raw::Fixed32(u32::from_le_bytes(bytes[s..e].try_into().expect("4 bytes")))
            }
            WireType::LengthDelimited => {
                let len = self.varint(pos, end)?;
                let (s, e) = self.take(pos, end, len)?;
                Raw::Len(&bytes[s..e])
            }
            WireType::StartGroup | WireType::EndGroup => {
                return Err(malformed("protobuf: unexpected group"))
            }
        })
    }

    /// Skip an undeclared field (groups structurally, counting depth).
    fn skip(
        &self,
        pos: &mut usize,
        end: usize,
        number: u32,
        wire: WireType,
        depth: usize,
    ) -> Result<()> {
        match wire {
            WireType::StartGroup => {
                let depth = self.deeper(depth)?;
                loop {
                    if *pos == end {
                        return Err(malformed("protobuf: unterminated group"));
                    }
                    let (inner, inner_wire) = self.key(pos, end)?;
                    if inner_wire == WireType::EndGroup {
                        if inner == number {
                            return Ok(());
                        }
                        return Err(malformed("protobuf: mismatched end-group"));
                    }
                    self.skip(pos, end, inner, inner_wire, depth)?;
                }
            }
            WireType::EndGroup => Err(malformed("protobuf: unexpected end-group")),
            wire => self.value(pos, end, wire).map(|_| ()),
        }
    }

    fn unknown_field(&self, desc: &str, number: u32) -> Result<()> {
        if self.unknown == UnknownFields::Error {
            return Err(err(
                ErrorCode::InvalidSchema,
                format!("protobuf: unknown field {number} in `{desc}`"),
            ));
        }
        Ok(())
    }

    /// Walk one message (`[pos, end)`, or up to the end-group of `group`).
    /// `node` is the plan node of this message when a column maps into it.
    fn message(
        &mut self,
        desc: &MessageDescriptor,
        node: Option<usize>,
        pos: &mut usize,
        end: usize,
        group: Option<u32>,
        depth: usize,
    ) -> Result<()> {
        let plan = self.plan;
        loop {
            if *pos == end {
                if group.is_some() {
                    return Err(malformed("protobuf: unterminated group"));
                }
                return Ok(());
            }
            let (number, wire) = self.key(pos, end)?;
            if wire == WireType::EndGroup {
                if group == Some(number) {
                    return Ok(());
                }
                return Err(malformed("protobuf: unexpected end-group"));
            }
            let Some(info) = field_info(desc, number) else {
                self.unknown_field(desc.full_name(), number)?;
                self.skip(pos, end, number, wire, depth)?;
                continue;
            };
            // Setting a oneof member clears the mapped other members.
            if let Some(oneof) = desc.get_field(number).and_then(|f| f.containing_oneof()) {
                if let Some((_, members)) = node.and_then(|n| {
                    plan.nodes[n]
                        .oneofs
                        .iter()
                        .find(|(name, _)| name == oneof.full_name())
                }) {
                    for &(other, slot) in members {
                        if other != number {
                            self.clear(slot);
                        }
                    }
                }
            }
            let slot = node.and_then(|n| plan.slot(n, number));
            self.field(desc, number, wire, &info, slot, pos, end, depth)?;
        }
    }

    fn clear(&mut self, slot: Slot) {
        match slot {
            Slot::Leaf(i) => self.leaves[i] = None,
            Slot::Node(j) => {
                self.present[j] = false;
                // Mapping depth is bounded to 100. Traverse the tree rather
                // than retaining a copy of every subtree at every ancestor.
                let plan = self.plan;
                for &(_, child) in &plan.nodes[j].slots {
                    self.clear(child);
                }
            }
        }
    }

    fn mismatch(desc: &MessageDescriptor, number: u32, wire: WireType) -> SparrowError {
        malformed(format!(
            "protobuf: field {number} of `{}` has wire type {wire:?}, which its declared type does not use",
            desc.full_name()
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn field(
        &mut self,
        desc: &MessageDescriptor,
        number: u32,
        wire: WireType,
        info: &FieldInfo,
        slot: Option<Slot>,
        pos: &mut usize,
        end: usize,
        depth: usize,
    ) -> Result<()> {
        if let Kind::Message(child) = &info.kind {
            if info.group {
                if wire != WireType::StartGroup {
                    return Err(Self::mismatch(desc, number, wire));
                }
                let depth = self.deeper(depth)?;
                return self.message(child, None, pos, end, Some(number), depth);
            }
            if wire != WireType::LengthDelimited {
                return Err(Self::mismatch(desc, number, wire));
            }
            let len = self.varint(pos, end)?;
            let (mut inner, inner_end) = self.take(pos, end, len)?;
            let depth = self.deeper(depth)?;
            let singular = !info.list && !info.map;
            return match slot {
                Some(Slot::Node(j)) if singular => {
                    self.present[j] = true;
                    self.message(child, Some(j), &mut inner, inner_end, None, depth)
                }
                Some(Slot::Leaf(i)) if singular => {
                    self.timestamp(child, i, &mut inner, inner_end, depth)
                }
                _ => self.message(child, None, &mut inner, inner_end, None, depth),
            };
        }
        let scalar_wire = info.kind.wire_type();
        if info.list && wire == WireType::LengthDelimited && scalar_wire != wire {
            // Packed repeated scalars: validate the element stream.
            let len = self.varint(pos, end)?;
            let (mut inner, inner_end) = self.take(pos, end, len)?;
            let width = match scalar_wire {
                WireType::SixtyFourBit => 8,
                WireType::ThirtyTwoBit => 4,
                _ => 0,
            };
            if width == 0 {
                while inner < inner_end {
                    let value = self.varint(&mut inner, inner_end)?;
                    check_closed_enum(&info.kind, value)?;
                }
            } else if (inner_end - inner) % width != 0 {
                return Err(malformed(
                    "protobuf: packed field length is not a whole number of elements",
                ));
            }
            return Ok(());
        }
        if wire != scalar_wire {
            return Err(Self::mismatch(desc, number, wire));
        }
        let raw = self.value(pos, end, wire)?;
        if let Raw::Varint(value) = raw {
            check_closed_enum(&info.kind, value)?;
        }
        if let (Kind::String, Raw::Len(text)) = (&info.kind, raw) {
            if std::str::from_utf8(text).is_err() {
                return Err(malformed(format!(
                    "protobuf: string field {number} of `{}` is not valid UTF-8",
                    desc.full_name()
                )));
            }
        }
        if let Some(Slot::Leaf(i)) = slot {
            if !info.list {
                self.leaves[i] = Some(raw);
            }
        }
        Ok(())
    }

    /// A mapped `google.protobuf.Timestamp` at `depth`: seconds and nanos
    /// each last-wins across occurrences, as a message merge does.
    fn timestamp(
        &mut self,
        desc: &MessageDescriptor,
        leaf: usize,
        pos: &mut usize,
        end: usize,
        depth: usize,
    ) -> Result<()> {
        let (mut seconds, mut nanos) = match self.leaves[leaf] {
            Some(Raw::Time { seconds, nanos }) => (seconds, nanos),
            _ => (0, 0),
        };
        while *pos < end {
            let (number, wire) = self.key(pos, end)?;
            match (number, wire) {
                (1, WireType::Varint) => seconds = self.varint(pos, end)? as i64,
                (2, WireType::Varint) => nanos = self.varint(pos, end)? as i32,
                (1 | 2, wire) => return Err(Self::mismatch(desc, number, wire)),
                (_, WireType::EndGroup) => return Err(malformed("protobuf: unexpected end-group")),
                (number, wire) => {
                    self.unknown_field(TIMESTAMP, number)?;
                    self.skip(pos, end, number, wire, depth)?;
                }
            }
        }
        self.leaves[leaf] = Some(Raw::Time { seconds, nanos });
        Ok(())
    }
}

fn type_err(leaf: &Leaf, what: impl std::fmt::Display) -> SparrowError {
    err(
        ErrorCode::TypeMismatch,
        format!(
            "protobuf column '{}' (`{}`): {what}",
            leaf.column, leaf.path
        ),
    )
}

fn zigzag32(v: u32) -> i32 {
    ((v >> 1) as i32) ^ -((v & 1) as i32)
}

fn zigzag64(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

fn convert(
    leaf: &Leaf,
    field: &Field,
    raw: Raw<'_>,
    owner: Option<&MemoryOwner>,
) -> Result<Scalar> {
    use ScalarKind as S;
    let ty = &field.data_type;
    let int = |v: i64| match ty {
        DataType::TimestampMicrosUTC => Scalar::TimestampMicrosUTC(v),
        _ => Scalar::Int64(v),
    };
    let unsigned = |v: u32| match ty {
        DataType::UInt64 => Scalar::UInt64(u64::from(v)),
        _ => Scalar::Int64(i64::from(v)),
    };
    let float = |v: f64| {
        if v.is_finite() {
            Ok(Scalar::Float64(v))
        } else {
            Err(type_err(leaf, "non-finite float"))
        }
    };
    let text = |s: &str| match owner {
        Some(o) => Scalar::utf8_tracked(o, s),
        None => Ok(Scalar::utf8(s)),
    };
    match (&leaf.kind, raw) {
        (LeafKind::Scalar(S::Int32), Raw::Varint(v)) => Ok(int(i64::from(v as i32))),
        (LeafKind::Scalar(S::Int64), Raw::Varint(v)) => Ok(int(v as i64)),
        (LeafKind::Scalar(S::Sint32), Raw::Varint(v)) => Ok(int(i64::from(zigzag32(v as u32)))),
        (LeafKind::Scalar(S::Sint64), Raw::Varint(v)) => Ok(int(zigzag64(v))),
        (LeafKind::Scalar(S::Uint32), Raw::Varint(v)) => Ok(unsigned(v as u32)),
        (LeafKind::Scalar(S::Uint64), Raw::Varint(v)) => Ok(Scalar::UInt64(v)),
        (LeafKind::Scalar(S::Bool), Raw::Varint(v)) => Ok(Scalar::Bool(v != 0)),
        (LeafKind::Scalar(S::Fixed32), Raw::Fixed32(v)) => Ok(unsigned(v)),
        (LeafKind::Scalar(S::Sfixed32), Raw::Fixed32(v)) => Ok(int(i64::from(v as i32))),
        (LeafKind::Scalar(S::Float), Raw::Fixed32(v)) => float(f64::from(f32::from_bits(v))),
        (LeafKind::Scalar(S::Fixed64), Raw::Fixed64(v)) => Ok(Scalar::UInt64(v)),
        (LeafKind::Scalar(S::Sfixed64), Raw::Fixed64(v)) => Ok(int(v as i64)),
        (LeafKind::Scalar(S::Double), Raw::Fixed64(v)) => float(f64::from_bits(v)),
        (LeafKind::Scalar(S::String), Raw::Len(b)) => {
            text(std::str::from_utf8(b).map_err(|_| type_err(leaf, "not valid UTF-8"))?)
        }
        (LeafKind::Scalar(S::Bytes), Raw::Len(b)) => match owner {
            Some(o) => Scalar::bytes_tracked(o, b),
            None => Ok(Scalar::bytes(b)),
        },
        (LeafKind::Enum { desc, closed }, Raw::Varint(v)) => {
            let number = v as i32;
            match (desc.get_value(number), ty) {
                (Some(value), DataType::Utf8) => text(value.name()),
                (None, DataType::Utf8) => Err(type_err(
                    leaf,
                    format!("{number} is not a value of enum `{}`", desc.full_name()),
                )),
                (None, _) if *closed => Err(type_err(
                    leaf,
                    format!(
                        "{number} is not a value of closed enum `{}`",
                        desc.full_name()
                    ),
                )),
                _ => Ok(Scalar::Int64(i64::from(number))),
            }
        }
        (LeafKind::Timestamp, Raw::Time { seconds, nanos }) => {
            if !(MIN_TIMESTAMP_SECONDS..=MAX_TIMESTAMP_SECONDS).contains(&seconds)
                || !(0..=999_999_999).contains(&nanos)
            {
                return Err(type_err(leaf, "Timestamp out of range"));
            }
            if nanos % 1000 != 0 {
                return Err(type_err(leaf, "Timestamp has sub-microsecond precision"));
            }
            Ok(Scalar::TimestampMicrosUTC(
                seconds * 1_000_000 + i64::from(nanos / 1000),
            ))
        }
        _ => Err(type_err(leaf, "wire value does not match the field type")),
    }
}

fn default_raw(kind: &LeafKind) -> Raw<'static> {
    match kind.wire() {
        WireType::SixtyFourBit => Raw::Fixed64(0),
        WireType::ThirtyTwoBit => Raw::Fixed32(0),
        WireType::LengthDelimited => Raw::Len(&[]),
        _ => Raw::Varint(0),
    }
}

impl ProtobufFormat {
    /// Cold plan construction, cached schema clone, enum-name expansion
    /// and per-node traversal arrays. Count path nodes, not just columns:
    /// one mapped leaf can sit below 99 recursive messages. No allocation
    /// or cache mutation is allowed while asking for admission credit.
    fn plan_scratch(&self, fields: usize) -> usize {
        let nodes = self
            .options
            .fields
            .values()
            .fold(1usize, |n, path| {
                n.saturating_add(path.bytes().filter(|b| *b == b'.').count())
            })
            .min(MAX_PLAN_NODES);
        fields.saturating_add(nodes).saturating_mul(4096)
    }

    /// One message -> one row. Rejects by length before any allocation.
    pub fn decode_message(
        &self,
        schema: &Schema,
        bytes: &[u8],
        owner: Option<&MemoryOwner>,
    ) -> Result<Row> {
        if bytes.len() > self.limits.max_message_bytes {
            return Err(err(
                ErrorCode::MaxRecordSize,
                format!(
                    "protobuf message {}B exceeds max_message_bytes {}",
                    bytes.len(),
                    self.limits.max_message_bytes
                ),
            ));
        }
        let plan = self.plan(schema)?;
        let mut walker = Walker {
            plan: &plan,
            bytes,
            unknown: self.options.unknown_fields,
            max_depth: self.limits.max_depth,
            leaves: vec![None; plan.leaves.len()],
            present: vec![false; plan.nodes.len()],
        };
        walker.present[0] = true;
        let mut pos = 0;
        walker.message(&self.message, Some(0), &mut pos, bytes.len(), None, 1)?;
        let mut values = Vec::with_capacity(schema.fields.len());
        for (i, (leaf, field)) in plan.leaves.iter().zip(&schema.fields).enumerate() {
            let mut node = Some(leaf.node);
            let mut reachable = true;
            while let Some(j) = node {
                reachable &= walker.present[j];
                node = plan.nodes[j].parent;
            }
            let raw = match walker.leaves[i] {
                Some(raw) if reachable => Some(raw),
                None if reachable && !leaf.presence => Some(default_raw(&leaf.kind)),
                _ => None,
            };
            values.push(match raw {
                Some(raw) => convert(leaf, field, raw, owner)?,
                None if field.nullable => Scalar::Null,
                None => return Err(type_err(leaf, "absent, and the column is not nullable")),
            });
        }
        Ok(Row { values })
    }

    /// A length-delimited stream (`varint length` + message, repeated): the
    /// document form of HTTP Poll responses and HTTP Sink bodies.
    pub fn document<'a>(&self, bytes: &'a [u8]) -> ProtobufDocument<'a> {
        ProtobufDocument { bytes, pos: 0 }
    }

    /// Conservative decode working set for one `len`-byte message, charged
    /// before decoding: Utf8/Bytes values copied out of the message (at most
    /// `len` in total plus an allocation header each), the output row, the
    /// per-column raw state and per-message presence, the plan (in case the
    /// call builds it) and slack. `tests/protobuf_alloc.rs` checks this
    /// against a counting allocator on adversarial messages.
    pub fn decode_scratch(&self, schema: &Schema, len: usize) -> usize {
        let fields = schema.fields.len();
        let per_field = std::mem::size_of::<Scalar>()
            + std::mem::size_of::<Option<Raw<'static>>>()
            + std::mem::size_of::<bool>()
            + 32;
        len.saturating_add(fields.saturating_mul(per_field))
            .saturating_add(self.plan_scratch(fields))
            .saturating_add(4096)
    }

    /// Conservative encoder scratch outside the bounded output buffer: the
    /// per-column wire values and per-message sizes (values are borrowed from
    /// the row, never copied), the plan (in case the call builds it), slack.
    pub fn encode_scratch(&self, row: &Row) -> usize {
        let fields = row.values.len();
        let per_field = std::mem::size_of::<Option<Enc<'static>>>()
            + std::mem::size_of::<usize>()
            + 2 * std::mem::size_of::<bool>();
        fields
            .saturating_mul(per_field)
            .saturating_add(self.plan_scratch(fields))
            .saturating_add(4096)
    }

    /// Canonical identity for durable checkpoints: role, descriptor bytes,
    /// message name, the mapping (entries equal to the default same-name
    /// mapping are dropped, so writing them out does not change it) and the
    /// effective decode policy and limits.
    pub fn identity_bytes(&self) -> Vec<u8> {
        fn put(out: &mut Vec<u8>, bytes: &[u8]) {
            out.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
            out.extend_from_slice(bytes);
        }
        let mut out = b"sparrow-protobuf-v1".to_vec();
        out.push(match self.role {
            FormatRole::Decode => 0,
            FormatRole::Encode => 1,
        });
        put(&mut out, &self.descriptor);
        put(&mut out, self.options.message.as_bytes());
        let mapped: Vec<_> = self
            .options
            .fields
            .iter()
            .filter(|(column, path)| column != path)
            .collect();
        out.extend_from_slice(&(mapped.len() as u64).to_le_bytes());
        for (column, path) in mapped {
            put(&mut out, column.as_bytes());
            put(&mut out, path.as_bytes());
        }
        out.push(self.options.unknown_fields as u8);
        out.extend_from_slice(&(self.limits.max_message_bytes as u64).to_le_bytes());
        out.extend_from_slice(&(self.limits.max_depth as u64).to_le_bytes());
        out
    }
}

/// Iterator over a length-delimited message stream.
pub struct ProtobufDocument<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> ProtobufDocument<'a> {
    /// The next message, or a framing error (truncated length or body) after
    /// which the stream ends. An over-long message is returned as is:
    /// [`ProtobufFormat::decode_message`] rejects it by length, and the
    /// stream continues after it.
    pub fn next_message(&mut self) -> Option<Result<&'a [u8]>> {
        if self.pos >= self.bytes.len() {
            return None;
        }
        let mut rest = &self.bytes[self.pos..];
        let framed = decode_varint(&mut rest)
            .ok()
            .and_then(|len| usize::try_from(len).ok())
            .filter(|len| *len <= rest.len());
        let Some(len) = framed else {
            self.pos = self.bytes.len();
            return Some(Err(malformed(
                "protobuf stream: truncated length-delimited message",
            )));
        };
        let start = self.bytes.len() - rest.len();
        self.pos = start + len;
        Some(Ok(&self.bytes[start..start + len]))
    }
}

fn check_closed_enum(kind: &Kind, value: u64) -> Result<()> {
    if let Kind::Enum(e) = kind {
        if e.parent_file().syntax() == Syntax::Proto2 && e.get_value(value as i32).is_none() {
            return Err(err(
                ErrorCode::TypeMismatch,
                "protobuf closed enum contains an unknown value",
            ));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Encode.

/// One wire value, borrowing strings/bytes from the row.
#[derive(Clone, Copy, Debug)]
enum Enc<'r> {
    Varint(u64),
    Fixed32(u32),
    Fixed64(u64),
    Len(&'r [u8]),
    Time { seconds: i64, nanos: i32 },
}

impl Enc<'_> {
    fn time_body(seconds: i64, nanos: i32) -> usize {
        let mut body = 0;
        if seconds != 0 {
            body += 1 + encoded_len_varint(seconds as u64);
        }
        if nanos != 0 {
            body += 1 + encoded_len_varint(nanos as i64 as u64);
        }
        body
    }

    /// Encoded length after the key (<= 10 + payload; payload <= 64 KiB per
    /// row value is not assumed: callers sum with checked math).
    fn len(&self) -> usize {
        match *self {
            Self::Varint(v) => encoded_len_varint(v),
            Self::Fixed32(_) => 4,
            Self::Fixed64(_) => 8,
            Self::Len(b) => encoded_len_varint(b.len() as u64).saturating_add(b.len()),
            Self::Time { seconds, nanos } => {
                let body = Self::time_body(seconds, nanos);
                encoded_len_varint(body as u64) + body
            }
        }
    }

    /// The implicit-presence default (bit-wise for floats, as protoc).
    fn is_default(&self) -> bool {
        match *self {
            Self::Varint(v) | Self::Fixed64(v) => v == 0,
            Self::Fixed32(v) => v == 0,
            Self::Len(b) => b.is_empty(),
            Self::Time { .. } => false,
        }
    }

    fn write(&self, out: &mut Vec<u8>) {
        match *self {
            Self::Varint(v) => encode_varint(v, out),
            Self::Fixed32(v) => out.extend_from_slice(&v.to_le_bytes()),
            Self::Fixed64(v) => out.extend_from_slice(&v.to_le_bytes()),
            Self::Len(b) => {
                encode_varint(b.len() as u64, out);
                out.extend_from_slice(b);
            }
            Self::Time { seconds, nanos } => {
                encode_varint(Self::time_body(seconds, nanos) as u64, out);
                if seconds != 0 {
                    encode_key(1, WireType::Varint, out);
                    encode_varint(seconds as u64, out);
                }
                if nanos != 0 {
                    encode_key(2, WireType::Varint, out);
                    encode_varint(nanos as i64 as u64, out);
                }
            }
        }
    }
}

fn zigzag_encode32(v: i32) -> u64 {
    u64::from(((v << 1) ^ (v >> 31)) as u32)
}

fn zigzag_encode64(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

/// One row value -> wire value (`None`: omit). No silent clamping: a value
/// outside the field's range is a type error.
fn encode_value<'r>(leaf: &Leaf, value: &'r Scalar) -> Result<Option<Enc<'r>>> {
    use ScalarKind as S;
    let range = |what: &str| type_err(leaf, format!("{what} is out of range for the field"));
    let int = match value {
        Scalar::Int64(v) | Scalar::TimestampMicrosUTC(v) => Some(*v),
        _ => None,
    };
    let unsigned = match value {
        Scalar::UInt64(v) => Some(*v),
        Scalar::Int64(v) => u64::try_from(*v).ok(),
        _ => None,
    };
    let i32_of = |v: i64| i32::try_from(v).map_err(|_| range(&v.to_string()));
    let enc = match (&leaf.kind, value) {
        (_, Scalar::Null) => return Ok(None),
        (LeafKind::Scalar(S::Float | S::Double), Scalar::Float64(v)) if !v.is_finite() => {
            if leaf.presence {
                return Ok(None);
            }
            return Err(type_err(
                leaf,
                "non-finite float cannot be written to a field without presence",
            ));
        }
        (LeafKind::Scalar(S::Int32), _) if int.is_some() => {
            Enc::Varint(i64::from(i32_of(int.unwrap())?) as u64)
        }
        (LeafKind::Scalar(S::Sint32), _) if int.is_some() => {
            Enc::Varint(zigzag_encode32(i32_of(int.unwrap())?))
        }
        (LeafKind::Scalar(S::Sfixed32), _) if int.is_some() => {
            Enc::Fixed32(i32_of(int.unwrap())? as u32)
        }
        (LeafKind::Scalar(S::Int64), _) if int.is_some() => Enc::Varint(int.unwrap() as u64),
        (LeafKind::Scalar(S::Sint64), _) if int.is_some() => {
            Enc::Varint(zigzag_encode64(int.unwrap()))
        }
        (LeafKind::Scalar(S::Sfixed64), _) if int.is_some() => Enc::Fixed64(int.unwrap() as u64),
        (LeafKind::Scalar(S::Uint32 | S::Fixed32), Scalar::Int64(_) | Scalar::UInt64(_)) => {
            let v = unsigned
                .and_then(|v| u32::try_from(v).ok())
                .ok_or_else(|| range(&format!("{value:?}")))?;
            match leaf.kind {
                LeafKind::Scalar(S::Uint32) => Enc::Varint(u64::from(v)),
                _ => Enc::Fixed32(v),
            }
        }
        (LeafKind::Scalar(S::Uint64), Scalar::UInt64(v)) => Enc::Varint(*v),
        (LeafKind::Scalar(S::Fixed64), Scalar::UInt64(v)) => Enc::Fixed64(*v),
        (LeafKind::Scalar(S::Float), Scalar::Float64(v)) => {
            let f = *v as f32;
            if f.is_infinite() {
                return Err(range(&v.to_string()));
            }
            Enc::Fixed32(f.to_bits())
        }
        (LeafKind::Scalar(S::Double), Scalar::Float64(v)) => Enc::Fixed64(v.to_bits()),
        (LeafKind::Scalar(S::Bool), Scalar::Bool(v)) => Enc::Varint(u64::from(*v)),
        (LeafKind::Scalar(S::String), Scalar::Utf8(v)) => Enc::Len(v.as_bytes()),
        (LeafKind::Scalar(S::Bytes), Scalar::Bytes(v)) => Enc::Len(v),
        (LeafKind::Enum { desc, closed }, Scalar::Int64(v)) => {
            let number = i32_of(*v)?;
            if *closed && desc.get_value(number).is_none() {
                return Err(type_err(
                    leaf,
                    format!(
                        "{number} is not a value of closed enum `{}`",
                        desc.full_name()
                    ),
                ));
            }
            Enc::Varint(i64::from(number) as u64)
        }
        (LeafKind::Enum { desc, .. }, Scalar::Utf8(name)) => {
            let value = desc.get_value_by_name(name).ok_or_else(|| {
                type_err(
                    leaf,
                    format!("`{name}` is not a value of enum `{}`", desc.full_name()),
                )
            })?;
            Enc::Varint(i64::from(value.number()) as u64)
        }
        (LeafKind::Timestamp, Scalar::TimestampMicrosUTC(micros)) => {
            let seconds = micros.div_euclid(1_000_000);
            if !(MIN_TIMESTAMP_SECONDS..=MAX_TIMESTAMP_SECONDS).contains(&seconds) {
                return Err(range(&micros.to_string()));
            }
            Enc::Time {
                seconds,
                nanos: (micros.rem_euclid(1_000_000) * 1000) as i32,
            }
        }
        _ => {
            return Err(type_err(
                leaf,
                format!("{:?} value does not match the column", value.data_type()),
            ))
        }
    };
    Ok(Some(enc))
}

/// Wire values and message sizes of one row (strings borrowed).
struct Encoded<'r> {
    values: Vec<Option<Enc<'r>>>,
    emitted: Vec<bool>,
    sizes: Vec<usize>,
}

fn too_large() -> SparrowError {
    err(
        ErrorCode::BoundExceeded,
        "protobuf message size overflows usize",
    )
}

impl ProtobufFormat {
    fn prepare<'r>(&self, plan: &Plan, schema: &Schema, row: &'r Row) -> Result<Encoded<'r>> {
        if row.values.len() != schema.fields.len() {
            return Err(err(
                ErrorCode::TypeMismatch,
                format!(
                    "row has {} values for {} schema fields",
                    row.values.len(),
                    schema.fields.len()
                ),
            ));
        }
        let mut values = Vec::with_capacity(plan.leaves.len());
        for ((leaf, field), value) in plan.leaves.iter().zip(&schema.fields).zip(&row.values) {
            if value.is_null() && !field.nullable {
                return Err(type_err(leaf, "NULL in a non-nullable column"));
            }
            let enc = encode_value(leaf, value)?;
            if enc.is_none() && !leaf.presence {
                return Err(type_err(
                    leaf,
                    "NULL cannot be written to a field without presence",
                ));
            }
            values.push(enc);
        }
        let mut emitted = vec![false; plan.nodes.len()];
        for j in (0..plan.nodes.len()).rev() {
            emitted[j] = plan.nodes[j].slots.iter().any(|(_, slot)| match *slot {
                Slot::Leaf(i) => values[i].is_some(),
                Slot::Node(k) => emitted[k],
            });
        }
        emitted[0] = true;
        let set = |slot: Slot| match slot {
            Slot::Leaf(i) => values[i].is_some(),
            Slot::Node(j) => emitted[j],
        };
        for node in &plan.nodes {
            for (name, members) in &node.oneofs {
                if members.iter().filter(|&&(_, s)| set(s)).count() > 1 {
                    return Err(err(
                        ErrorCode::TypeMismatch,
                        format!("more than one member of oneof `{name}` is non-null"),
                    ));
                }
            }
        }
        let mut sizes = vec![0usize; plan.nodes.len()];
        for j in (0..plan.nodes.len()).rev() {
            let mut size = 0usize;
            for &(number, slot) in &plan.nodes[j].slots {
                let body = match slot {
                    Slot::Leaf(i) => match values[i] {
                        Some(enc) if plan.leaves[i].presence || !enc.is_default() => enc.len(),
                        _ => continue,
                    },
                    Slot::Node(k) if emitted[k] => encoded_len_varint(sizes[k] as u64)
                        .checked_add(sizes[k])
                        .ok_or_else(too_large)?,
                    Slot::Node(_) => continue,
                };
                size = size
                    .checked_add(key_len(number))
                    .and_then(|n| n.checked_add(body))
                    .ok_or_else(too_large)?;
            }
            sizes[j] = size;
        }
        Ok(Encoded {
            values,
            emitted,
            sizes,
        })
    }

    fn write(&self, plan: &Plan, encoded: &Encoded<'_>, node: usize, out: &mut Vec<u8>) {
        for &(number, slot) in &plan.nodes[node].slots {
            match slot {
                Slot::Leaf(i) => match encoded.values[i] {
                    Some(enc) if plan.leaves[i].presence || !enc.is_default() => {
                        encode_key(number, plan.leaves[i].kind.wire(), out);
                        enc.write(out);
                    }
                    _ => {}
                },
                Slot::Node(k) if encoded.emitted[k] => {
                    encode_key(number, WireType::LengthDelimited, out);
                    encode_varint(encoded.sizes[k] as u64, out);
                    self.write(plan, encoded, k, out);
                }
                Slot::Node(_) => {}
            }
        }
    }

    fn allocate(
        total: usize,
        limit: usize,
        mut admit: impl FnMut(usize) -> Result<()>,
    ) -> Result<Vec<u8>> {
        if total > limit {
            return Err(err(
                ErrorCode::BoundExceeded,
                format!("protobuf output {total}B exceeds the {limit}B limit"),
            ));
        }
        admit(total)?;
        let mut out = Vec::new();
        out.try_reserve_exact(total).map_err(|e| {
            err(
                ErrorCode::ResourceExhausted,
                format!("protobuf buffer allocation: {e}"),
            )
        })?;
        if out.capacity() > total {
            return Err(err(
                ErrorCode::BoundExceeded,
                "protobuf allocator exceeded the admitted output capacity",
            ));
        }
        Ok(out)
    }

    /// One row -> one message, under a hard byte bound. The exact size is
    /// computed first, so `admit` is called once with the exact capacity
    /// and an over-long message is refused before allocating.
    pub fn encode_message_bounded_with_capacity(
        &self,
        schema: &Schema,
        row: &Row,
        limit: usize,
        admit: impl FnMut(usize) -> Result<()>,
    ) -> Result<Vec<u8>> {
        let plan = self.plan(schema)?;
        let encoded = self.prepare(&plan, schema, row)?;
        let mut out = Self::allocate(encoded.sizes[0], limit, admit)?;
        self.write(&plan, &encoded, 0, &mut out);
        debug_assert_eq!(out.len(), encoded.sizes[0]);
        Ok(out)
    }

    pub fn encode_message(&self, schema: &Schema, row: &Row) -> Result<Vec<u8>> {
        self.encode_message_bounded_with_capacity(schema, row, usize::MAX, |_| Ok(()))
    }

    /// Rows -> a length-delimited stream (HTTP Sink body). Two passes: sizes
    /// first (refusing over `limit` before allocating), then the bytes.
    pub fn encode_rows_bounded_with_capacity(
        &self,
        schema: &Schema,
        rows: &[Row],
        limit: usize,
        admit: impl FnMut(usize) -> Result<()>,
    ) -> Result<Vec<u8>> {
        let plan = self.plan(schema)?;
        let mut total = 0usize;
        for row in rows {
            let size = self.prepare(&plan, schema, row)?.sizes[0];
            total = total
                .checked_add(encoded_len_varint(size as u64))
                .and_then(|n| n.checked_add(size))
                .ok_or_else(too_large)?;
        }
        let mut out = Self::allocate(total, limit, admit)?;
        for row in rows {
            let encoded = self.prepare(&plan, schema, row)?;
            encode_varint(encoded.sizes[0] as u64, &mut out);
            self.write(&plan, &encoded, 0, &mut out);
        }
        debug_assert_eq!(out.len(), total);
        Ok(out)
    }
}

#[cfg(test)]
#[path = "protobuf_tests.rs"]
mod tests;
