//! Bounded, typed, length-framed state semantics. Hashes are diagnostics only.
//! Field order and Dynamic object order are significant; float bits are exact.

use crate::{PhysicalPlan, PhysicalStage, TransformStep, WindowSpec};
use crate::stateful::{DeadbandBaseline, DeadbandMode, InvalidValuePolicy, IotSpec};
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

/// Full diagnostic computation plus the prefix affecting restored state.
/// In a linear plan, the last window's prefix contains every earlier window's
/// dependencies. A stateless plan depends only on the source identity/schema.
pub(crate) fn checkpoint_pipeline(plan: &PhysicalPlan) -> Result<(Vec<u8>, usize)> {
    let mut w = Writer::default();
    w.raw(b"CP01")?;
    w.raw(&sparrow_expr::semantics::VERSION.to_le_bytes())?;
    w.bytes(sparrow_expr::semantics::EVALUATION.as_bytes())?;
    let mut recovery_prefix_len = 0;
    for stage in &plan.stages {
        match stage {
            PhysicalStage::MemorySource { operator, name, schema } => {
                w.tag(0)?; w.raw(&operator.raw().to_le_bytes())?; w.bytes(name.as_bytes())?; w.schema(schema)?;
                recovery_prefix_len = w.0.len();
            }
            PhysicalStage::Transform { steps } => for step in steps {
                match step {
                    TransformStep::Filter { predicate, input, .. } => {
                        w.tag(1)?; w.schema(input)?; w.expr(predicate,0)?;
                    }
                    TransformStep::Project { exprs, input, output, .. }
                    | TransformStep::Map { exprs, input, output, .. } => {
                        w.tag(if matches!(step, TransformStep::Project { .. }) { 2 } else { 3 })?;
                        w.schema(input)?; w.schema(output)?; w.len(exprs.len())?;
                        for expr in exprs { w.expr(expr,0)?; }
                    }
                }
            },
            PhysicalStage::WindowAgg { operator, spec, input, output } => {
                w.tag(4)?; w.raw(&operator.raw().to_le_bytes())?;
                w.bytes(&StateSemantics::window(spec)?.encode()?)?;
                w.schema(input)?; w.schema(output)?;
                recovery_prefix_len = w.0.len();
            }
            PhysicalStage::Iot { operator, spec, input, output } => {
                // Tag 6 is new and is only emitted for an IoT state stage;
                // legacy plans never take this arm, so their bytes stay
                // byte-for-byte unchanged.  IoT recovery uses the complete
                // descriptor (no RCP2 downstream-prefix relaxation).
                w.tag(if spec.timing.is_some() { 9 } else if spec.hysteresis.is_some() { 8 } else { 6 })?; w.raw(&operator.raw().to_le_bytes())?;
                w.iot(spec, input)?; w.schema(input)?;
                if spec.is_alarm() || spec.is_silence() { w.schema(output)?; }
            }
            PhysicalStage::CaptureSink { operator, schema, .. } => {
                w.tag(5)?; w.raw(&operator.raw().to_le_bytes())?; w.schema(schema)?;
            }
            PhysicalStage::Lookup { operator, spec, input, output } => {
                // Only the CPL3/v8 profile admits this new tag. Old plans
                // retain byte-identical CP01 computation descriptors.
                w.tag(7)?; w.raw(&operator.raw().to_le_bytes())?;
                w.bytes(spec.table.as_bytes())?;
                for names in [&spec.stream_keys, &spec.table_keys, &spec.keep] {
                    w.len(names.len())?;
                    for name in names { w.bytes(name.as_bytes())?; }
                }
                w.tag(u8::from(spec.temporal))?;
                w.tag(u8::from(spec.as_of_field.is_some()))?;
                if let Some(name) = &spec.as_of_field { w.bytes(name.as_bytes())?; }
                w.schema(input)?; w.schema(output)?;
            }
            _ => return Err(SparrowError::new(ErrorCode::UnsupportedRestore,"unsupported checkpoint computation")),
        }
    }
    Ok((w.0, recovery_prefix_len))
}

/// Strict full-graph recovery identity. No downstream-only compatibility for
/// graphs: topology, routes, input schemas and every required sink are dependencies.
pub(crate) fn checkpoint_graph(plan: &PhysicalPlan, sources: &[sparrow_model::OperatorId], sinks: &[sparrow_model::OperatorId]) -> Result<Vec<u8>> {
    let mut w = Writer::default();
    let time_graph=plan.has_processing_time_state() || plan.has_event_time_window();
    w.raw(if time_graph {b"CP01DAG2"} else {b"CP01DAG1"})?;
    for ids in [sources, sinks] {
        w.raw(&(ids.len() as u16).to_le_bytes())?;
        for id in ids { w.raw(&id.raw().to_le_bytes())?; }
    }
    w.raw(&sparrow_expr::semantics::VERSION.to_le_bytes())?;
    w.bytes(sparrow_expr::semantics::EVALUATION.as_bytes())?;
    for stage in &plan.stages {
        match stage {
            PhysicalStage::Branch { operator, input } | PhysicalStage::UnionAll { operator, input } => {
                w.tag(if matches!(stage, PhysicalStage::Branch { .. }) { 10 } else { 11 })?;
                w.raw(&operator.raw().to_le_bytes())?; w.schema(input)?;
            }
            PhysicalStage::Route { operator, input, mode, cases, default } => {
                w.tag(12)?; w.raw(&operator.raw().to_le_bytes())?; w.schema(input)?;
                w.tag(u8::from(*mode == crate::graph::RouteMode::AllMatch))?;
                w.len(cases.len())?;
                for (expr, dest) in cases { w.expr(expr, 0)?; w.raw(&dest.raw().to_le_bytes())?; }
                w.raw(&default.raw().to_le_bytes())?;
            }
            _ => {
                let single = PhysicalPlan { pipeline: plan.pipeline, revision: plan.revision, stages: vec![stage.clone()], edges: None, side_outputs: vec![], source_times: vec![] };
                w.bytes(&checkpoint_pipeline(&single)?.0)?;
            }
        }
    }
    for edge in plan.edges.as_ref().expect("graph topology") {
        w.raw(&(edge.from as u32).to_le_bytes())?; w.raw(&(edge.to as u32).to_le_bytes())?;
        w.raw(&edge.port.raw().to_le_bytes())?; w.tag(u8::from(edge.best_effort))?;
    }
    if time_graph {
        w.bytes(b"durable-round-v1; fixed-edge-order; time-before-derived; observation-clock-recorded")?;
        w.len(plan.source_times.len())?;
        for (id,binding) in &plan.source_times {
            w.raw(&id.raw().to_le_bytes())?;w.bytes(binding.field.as_bytes())?;
            w.raw(&binding.out_of_orderness_micros.to_le_bytes())?;
            w.raw(&binding.max_future_skew_micros.unwrap_or(-1).to_le_bytes())?;
        }
    }
    Ok(w.0)
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

    fn iot(&mut self, spec: &IotSpec, input: &Schema) -> Result<()> {
        if let Some(timing) = &spec.timing {
            self.tag(timing.state_kind())?;
            // Source-ordered paused processing time, hashed from the
            // configuration instead of assumed, so a future clock policy cannot
            // silently reuse these bytes. `Paused` keeps emitting 1.
            let clock = match timing {
                crate::IotTimingSpec::Alarm { clock, .. }
                | crate::IotTimingSpec::HoldFor { clock, .. }
                | crate::IotTimingSpec::Debounce { clock, .. }
                | crate::IotTimingSpec::Silence { clock, .. } => match clock {
                    crate::ProcessingTimePolicy::Paused => 1u8,
                },
            };
            self.tag(clock)?;
            match timing {
                crate::IotTimingSpec::Alarm { activate_micros, resolve_micros, cooldown_micros, notification_max_age_micros, .. } => {
                    for duration in [activate_micros, resolve_micros, cooldown_micros, notification_max_age_micros] {
                        self.raw(&duration.to_le_bytes())?;
                    }
                }
                crate::IotTimingSpec::HoldFor { duration_micros, .. } => self.raw(&duration_micros.to_le_bytes())?,
                crate::IotTimingSpec::Debounce { quiet_micros, max_wait_micros, leading, trailing, reset_on_repeat, .. } => {
                    self.raw(&quiet_micros.to_le_bytes())?;
                    self.raw(&max_wait_micros.to_le_bytes())?;
                    self.tag(u8::from(*leading))?; self.tag(u8::from(*trailing))?; self.tag(u8::from(*reset_on_repeat))?;
                }
                crate::IotTimingSpec::Silence { duration_micros, max_observation_gap_micros, .. } => {
                    self.raw(&duration_micros.to_le_bytes())?;
                    self.raw(&max_observation_gap_micros.to_le_bytes())?;
                    // The registry is a set: sort the canonical key encodings so
                    // JSON row order cannot manufacture a false state mismatch,
                    // then write every key in full. Distinct registered device
                    // sets must never share one recovery identity, so this is
                    // not reduced to a hash. The overall state-semantics bound
                    // (64 KiB) applies: a registry that fills its own 64 KiB
                    // budget and leaves no room for the surrounding schema
                    // metadata is rejected explicitly, never shortened.
                    let mut rows = Vec::new();
                    for keys in spec.silence_registered_keys(input)? {
                        let mut encoded = Vec::new();
                        for key in &keys {
                            key.encode_key(&mut encoded);
                        }
                        rows.push(encoded);
                    }
                    rows.sort();
                    self.len(rows.len())?;
                    for row in &rows {
                        self.bytes(row)?;
                    }
                }
            }
        }
        self.len(spec.keys.len())?;
        for key in &spec.keys { self.bytes(key.as_bytes())?; }
        self.len(spec.fields.len())?;
        for field in &spec.fields { self.bytes(field.as_bytes())?; }
        self.tag(u8::from(spec.emit_first))?;
        self.raw(&spec.ttl_micros.to_le_bytes())?;
        self.raw(&(spec.max_keys as u64).to_le_bytes())?;
        self.tag(match spec.invalid {
            InvalidValuePolicy::Error => 0,
            InvalidValuePolicy::Ignore => 1,
        })?;
        match &spec.deadband {
            None => self.tag(0)?,
            Some(deadband) => {
                self.tag(1)?;
                self.tag(match deadband.mode {
                    DeadbandMode::Absolute => 0,
                    DeadbandMode::Relative => 1,
                })?;
                self.tag(match deadband.baseline {
                    DeadbandBaseline::LastInput => 0,
                    DeadbandBaseline::LastOutput => 1,
                })?;
                // -0.0 is the same threshold as +0.0 under the validated
                // >=0 contract; canonicalize it so config spelling cannot
                // manufacture a false state incompatibility.
                let bits = if deadband.threshold == 0.0 {
                    0.0f64.to_bits()
                } else {
                    deadband.threshold.to_bits()
                };
                self.raw(&bits.to_le_bytes())?;
            }
        }
        // No extra byte for old Change/Deadband plans. Tag 8 distinguishes
        // this new contract, and the outer profile rejects old readers.
        if let Some(hysteresis) = &spec.hysteresis {
            self.tag(match hysteresis.direction {
                crate::HysteresisDirection::High => 0,
                crate::HysteresisDirection::Low => 1,
            })?;
            for threshold in [hysteresis.enter, hysteresis.exit] {
                let bits = if threshold == 0.0 { 0.0f64.to_bits() } else { threshold.to_bits() };
                self.raw(&bits.to_le_bytes())?;
            }
        }
        Ok(())
    }
}

pub(crate) fn expression(expr: &Expr) -> Result<Vec<u8>> {
    let mut w = Writer::default();
    w.expr(expr, 0)?;
    Ok(w.0)
}
