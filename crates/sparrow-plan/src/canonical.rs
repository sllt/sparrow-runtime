//! Bounded, typed, length-framed state semantics. Hashes are diagnostics only.
//! Field order and Dynamic object order are significant; float bits are exact.

use crate::{PhysicalPlan, PhysicalStage, TransformStep, WindowSpec};
use sparrow_expr::Expr;
use sparrow_model::{
    DataType, DynamicValue, ErrorCode, Field, Result, Scalar, Schema, SparrowError, WindowKind,
};

pub const MAX_STATE_SEMANTICS_BYTES: usize = 64 * 1024;
const MAGIC: &[u8; 4] = b"SS02";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StateSemantics {
    // Window, standalone predicate, window input schema, upstream dataflow.
    parts: [Vec<u8>; 4],
}

impl StateSemantics {
    pub(crate) fn window(spec: &WindowSpec) -> Result<Self> {
        let mut w = Writer::default();
        match spec.kind {
            WindowKind::Count { size } => {
                w.tag(0)?;
                w.raw(&size.to_le_bytes())?;
            }
            WindowKind::TumblingProcessingTime { size_micros } => {
                w.tag(1)?;
                w.raw(&size_micros.to_le_bytes())?;
            }
            WindowKind::TumblingEventTime { size_micros } => {
                w.tag(2)?;
                w.raw(&size_micros.to_le_bytes())?;
            }
            WindowKind::HoppingEventTime {
                size_micros,
                slide_micros,
            } => {
                w.tag(3)?;
                w.raw(&size_micros.to_le_bytes())?;
                w.raw(&slide_micros.to_le_bytes())?;
            }
        }
        w.len(spec.keys.len())?;
        for key in &spec.keys {
            w.bytes(key.as_bytes())?;
        }
        w.len(spec.aggs.len())?;
        for agg in &spec.aggs {
            w.bytes(agg.func.as_str().as_bytes())?;
            w.bytes(agg.alias.as_bytes())?;
            w.tag(u8::from(agg.count_star))?;
            w.optional_expr(agg.input.as_ref())?;
        }
        w.tag(u8::from(spec.event_time_field.is_some()))?;
        if let Some(field) = &spec.event_time_field {
            w.bytes(field.as_bytes())?;
        }
        w.raw(&spec.lateness_micros.to_le_bytes())?;
        w.raw(&spec.max_overlap.to_le_bytes())?;
        w.raw(
            &spec
                .max_future_skew_micros
                .unwrap_or(sparrow_model::DEFAULT_MAX_FUTURE_SKEW_MICROS)
                .to_le_bytes(),
        )?;
        let s = Self {
            parts: [w.0, vec![0], Vec::new(), Vec::new()],
        };
        s.check_size()?;
        Ok(s)
    }

    pub(crate) fn predicate(&mut self, expr: Option<&Expr>) -> Result<()> {
        let mut w = Writer::default();
        w.optional_expr(expr)?;
        self.parts[1] = w.0;
        self.check_size()
    }

    pub(crate) fn input(&mut self, schema: &Schema) -> Result<()> {
        let mut w = Writer::default();
        w.schema(schema)?;
        self.parts[2] = w.0;
        self.check_size()
    }

    pub(crate) fn upstream(&mut self, plan: &PhysicalPlan) -> Result<()> {
        let mut w = Writer::default();
        for stage in &plan.stages {
            match stage {
                PhysicalStage::MemorySource { name, schema, .. } => {
                    w.tag(0)?;
                    w.bytes(name.as_bytes())?;
                    w.schema(schema)?;
                }
                PhysicalStage::Transform { steps } => {
                    for step in steps {
                        // Flatten fusion groups; IDs/backend choice are not semantics.
                        match step {
                            TransformStep::Filter {
                                predicate, input, ..
                            } => {
                                w.tag(1)?;
                                w.schema(input)?;
                                w.expr(predicate, 0)?;
                            }
                            TransformStep::Project {
                                exprs,
                                input,
                                output,
                                ..
                            }
                            | TransformStep::Map {
                                exprs,
                                input,
                                output,
                                ..
                            } => {
                                w.tag(if matches!(step, TransformStep::Project { .. }) {
                                    2
                                } else {
                                    3
                                })?;
                                w.schema(input)?;
                                w.schema(output)?;
                                w.len(exprs.len())?;
                                for expr in exprs {
                                    w.expr(expr, 0)?;
                                }
                            }
                        }
                    }
                }
                PhysicalStage::WindowAgg { .. } => break,
                _ => {
                    return Err(SparrowError::new(
                        ErrorCode::UnsupportedRestore,
                        "unsupported upstream state semantics",
                    ))
                }
            }
        }
        self.parts[3] = w.0;
        self.check_size()
    }

    fn check_size(&self) -> Result<()> {
        if self.parts.iter().map(Vec::len).sum::<usize>() + 20 > MAX_STATE_SEMANTICS_BYTES {
            return Err(bound());
        }
        Ok(())
    }

    /// Versioned metadata only, not executable code. Missing schemas are valid
    /// for construction but cannot authorize checkpoint reuse.
    pub fn has_input_schema(&self) -> bool {
        !self.parts[2].is_empty()
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        self.check_size()?;
        let mut w = Writer::default();
        w.raw(MAGIC)?;
        for part in &self.parts {
            w.bytes(part)?;
        }
        Ok(w.0)
    }

    pub fn decode(mut bytes: &[u8]) -> Result<Self> {
        let invalid = || {
            SparrowError::new(
                ErrorCode::CodecViolation,
                "invalid state semantics descriptor (SS02)",
            )
        };
        if bytes.len() > MAX_STATE_SEMANTICS_BYTES || !bytes.starts_with(MAGIC) {
            return Err(invalid());
        }
        bytes = &bytes[4..];
        let mut parts = std::array::from_fn(|_| Vec::new());
        for part in &mut parts {
            if bytes.len() < 4 {
                return Err(invalid());
            }
            let len = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
            bytes = &bytes[4..];
            if bytes.len() < len {
                return Err(invalid());
            }
            *part = bytes[..len].to_vec();
            bytes = &bytes[len..];
        }
        if !bytes.is_empty() || parts[0].is_empty() || parts[1].is_empty() {
            return Err(invalid());
        }
        Ok(Self { parts })
    }
}

fn bound() -> SparrowError {
    SparrowError::new(
        ErrorCode::BoundExceeded,
        "state semantics exceeds 64 KiB or nesting depth 64; aligned restore refused",
    )
}

#[derive(Default)]
struct Writer(Vec<u8>);
impl Writer {
    fn raw(&mut self, bytes: &[u8]) -> Result<()> {
        if self.0.len().saturating_add(bytes.len()) > MAX_STATE_SEMANTICS_BYTES {
            return Err(bound());
        }
        self.0.extend_from_slice(bytes);
        Ok(())
    }
    fn tag(&mut self, tag: u8) -> Result<()> {
        self.raw(&[tag])
    }
    fn len(&mut self, len: usize) -> Result<()> {
        if len > MAX_STATE_SEMANTICS_BYTES {
            return Err(bound());
        }
        self.raw(&(len as u32).to_le_bytes())
    }
    fn bytes(&mut self, bytes: &[u8]) -> Result<()> {
        self.len(bytes.len())?;
        self.raw(bytes)
    }
    fn depth(&self, depth: usize) -> Result<()> {
        if depth > 64 {
            Err(bound())
        } else {
            Ok(())
        }
    }
    fn schema(&mut self, schema: &Schema) -> Result<()> {
        // SchemaId is a catalog identity, not a row-layout property.
        self.fields(&schema.fields, 0)
    }
    fn fields(&mut self, fields: &[Field], depth: usize) -> Result<()> {
        self.depth(depth)?;
        self.len(fields.len())?;
        for f in fields {
            self.raw(&f.id.raw().to_le_bytes())?;
            self.bytes(f.name.as_bytes())?;
            self.tag(u8::from(f.nullable))?;
            self.ty(&f.data_type, depth + 1)?;
        }
        Ok(())
    }
    fn ty(&mut self, ty: &DataType, depth: usize) -> Result<()> {
        self.depth(depth)?;
        self.bytes(ty.name().as_bytes())?;
        match ty {
            DataType::Array(inner) => self.ty(inner, depth + 1),
            DataType::Struct(fields) => self.fields(fields, depth + 1),
            DataType::Map { key, value } => {
                self.ty(key, depth + 1)?;
                self.ty(value, depth + 1)
            }
            _ => Ok(()),
        }
    }
    fn scalar(&mut self, s: &Scalar, depth: usize) -> Result<()> {
        match s {
            Scalar::Null => self.tag(0),
            Scalar::Bool(v) => {
                self.tag(1)?;
                self.tag(u8::from(*v))
            }
            Scalar::Int64(v) => {
                self.tag(2)?;
                self.raw(&v.to_le_bytes())
            }
            Scalar::UInt64(v) => {
                self.tag(3)?;
                self.raw(&v.to_le_bytes())
            }
            Scalar::Float64(v) => {
                self.tag(4)?;
                self.raw(&v.to_bits().to_le_bytes())
            }
            Scalar::Utf8(v) => {
                self.tag(5)?;
                self.bytes(v.as_bytes())
            }
            Scalar::Bytes(v) => {
                self.tag(6)?;
                self.bytes(v)
            }
            Scalar::TimestampMicrosUTC(v) => {
                self.tag(7)?;
                self.raw(&v.to_le_bytes())
            }
            Scalar::Dynamic(v) => {
                self.tag(8)?;
                self.dynamic(v, depth + 1)
            }
        }
    }
    fn dynamic(&mut self, v: &DynamicValue, depth: usize) -> Result<()> {
        self.depth(depth)?;
        match v {
            DynamicValue::Null => self.tag(0),
            DynamicValue::Bool(v) => {
                self.tag(1)?;
                self.tag(u8::from(*v))
            }
            DynamicValue::Int64(v) => {
                self.tag(2)?;
                self.raw(&v.to_le_bytes())
            }
            DynamicValue::UInt64(v) => {
                self.tag(3)?;
                self.raw(&v.to_le_bytes())
            }
            DynamicValue::Float64(v) => {
                self.tag(4)?;
                self.raw(&v.to_bits().to_le_bytes())
            }
            DynamicValue::Utf8(v) => {
                self.tag(5)?;
                self.bytes(v.as_bytes())
            }
            DynamicValue::Bytes(v) => {
                self.tag(6)?;
                self.bytes(v)
            }
            DynamicValue::Array(v) => {
                self.tag(7)?;
                self.len(v.len())?;
                for item in v.iter() {
                    self.dynamic(item, depth + 1)?;
                }
                Ok(())
            }
            DynamicValue::Object(v) => {
                self.tag(8)?;
                self.len(v.len())?;
                for (key, value) in v.iter() {
                    self.bytes(key.as_bytes())?;
                    self.dynamic(value, depth + 1)?;
                }
                Ok(())
            }
        }
    }
    fn optional_expr(&mut self, expr: Option<&Expr>) -> Result<()> {
        self.tag(u8::from(expr.is_some()))?;
        if let Some(expr) = expr {
            self.expr(expr, 0)?;
        }
        Ok(())
    }
    fn expr(&mut self, expr: &Expr, depth: usize) -> Result<()> {
        self.depth(depth)?;
        match expr {
            Expr::Column { name } => {
                self.tag(0)?;
                self.bytes(name.as_bytes())
            }
            Expr::Literal(s) => {
                self.tag(1)?;
                self.scalar(s, depth)
            }
            Expr::Cast {
                expr: inner,
                target,
            }
            | Expr::TryCast {
                expr: inner,
                target,
            } => {
                self.tag(if matches!(expr, Expr::Cast { .. }) {
                    2
                } else {
                    3
                })?;
                self.expr(inner, depth + 1)?;
                self.ty(target, depth + 1)
            }
            Expr::Binary { op, left, right } => {
                self.tag(4)?;
                self.bytes(op.as_tag().as_bytes())?;
                self.expr(left, depth + 1)?;
                self.expr(right, depth + 1)
            }
            Expr::IsNull(inner) | Expr::IsNotNull(inner) | Expr::Not(inner) => {
                self.tag(match expr {
                    Expr::IsNull(_) => 5,
                    Expr::IsNotNull(_) => 6,
                    _ => 7,
                })?;
                self.expr(inner, depth + 1)
            }
            Expr::Call { name, args } => {
                self.tag(8)?;
                self.bytes(name.as_bytes())?;
                self.len(args.len())?;
                for arg in args {
                    self.expr(arg, depth + 1)?;
                }
                Ok(())
            }
            Expr::DynamicGet { expr, key } => {
                self.tag(9)?;
                self.expr(expr, depth + 1)?;
                self.bytes(key.as_bytes())
            }
        }
    }
}

pub(crate) fn expression(expr: &Expr) -> Result<Vec<u8>> {
    let mut w = Writer::default();
    w.expr(expr, 0)?;
    Ok(w.0)
}
