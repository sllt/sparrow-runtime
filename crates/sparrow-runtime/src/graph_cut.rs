//! Bounded durable graph control state. This is a separate source profile,
//! never an extension of the ready-order legacy File DAG cursor.
use sparrow_io::{SourceIdentity, SourcePosition};
use sparrow_model::{
    CreditKind, ErrorCode, MemoryLease, MemoryOwner, OutputSequence, Result, SparrowError,
};
use sparrow_plan::{CheckpointPlan, PhysicalPlan, PhysicalStage};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

pub const KIND: &str = "time-file-dag-v1";
pub const MAX_BYTES: usize = 30 * 1024;
pub fn invalid(message: &str) -> SparrowError {
    SparrowError::new(
        ErrorCode::UnsupportedRestore,
        format!("durable graph time: {message}"),
    )
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Progress {
    pub watermark: Option<i64>,
    pub idle: bool,
    pub eof: bool,
}
impl Progress {
    pub fn control(&self) -> crate::StreamControl {
        crate::StreamControl::GraphProgress {
            watermark_micros: self.watermark.unwrap_or(-1),
            flags: u8::from(self.idle) | (u8::from(self.eof) << 1),
        }
    }
    pub fn from_control(watermark_micros: i64, flags: u8) -> Result<Self> {
        if watermark_micros < -1 || flags > 3 {
            return Err(invalid("invalid graph progress control"));
        }
        let value = Self {
            watermark: (watermark_micros != -1).then_some(watermark_micros),
            idle: flags & 1 != 0,
            eof: flags & 2 != 0,
        };
        value.validate()?;
        Ok(value)
    }
    pub fn validate(&self) -> Result<()> {
        if self.watermark.is_some_and(|v| v < 0) || (self.eof && !self.idle) {
            return Err(invalid("invalid input progress"));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceProgress {
    pub position: SourcePosition,
    pub progress: Progress,
    pub last_input: i64,
    pub contract: u8,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnionProgress {
    pub inputs: Vec<Progress>,
    pub emitted: Progress,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphCut {
    pub sequence: u64,
    pub micros: i64,
    /// Recorded wall observation is used ONLY for ET future-skew validation.
    /// PT/TTL deadlines always use the independent paused logical clock.
    pub observed_micros: i64,
    pub ingested: u64,
    pub next_source: usize,
    pub idle_micros: Option<i64>,
    pub sources: BTreeMap<u32, SourceProgress>,
    pub unions: BTreeMap<u32, UnionProgress>,
    pub outputs: BTreeMap<u32, u64>,
}
fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}
fn put_progress(out: &mut Vec<u8>, p: &Progress) {
    put_u64(out, p.watermark.unwrap_or(-1) as u64);
    out.push(u8::from(p.idle));
    out.push(u8::from(p.eof));
}
fn take<'a>(src: &mut &'a [u8], n: usize) -> Result<&'a [u8]> {
    if src.len() < n {
        return Err(invalid("truncated graph cut"));
    }
    let (a, b) = src.split_at(n);
    *src = b;
    Ok(a)
}
fn number(src: &mut &[u8]) -> Result<u64> {
    Ok(u64::from_le_bytes(take(src, 8)?.try_into().unwrap()))
}
fn id(src: &mut &[u8]) -> Result<u32> {
    Ok(u32::from_le_bytes(take(src, 4)?.try_into().unwrap()))
}
fn count(src: &mut &[u8], cap: usize) -> Result<usize> {
    let n = id(src)? as usize;
    if n > cap {
        return Err(invalid("graph cut count limit"));
    }
    Ok(n)
}
fn text(src: &mut &[u8]) -> Result<String> {
    let n = count(src, MAX_BYTES)?;
    Ok(std::str::from_utf8(take(src, n)?)
        .map_err(|_| invalid("graph cut UTF8"))?
        .to_owned())
}
fn progress(src: &mut &[u8]) -> Result<Progress> {
    let wm = number(src)? as i64;
    let flags = take(src, 2)?;
    if wm < -1 || flags.iter().any(|v| *v > 1) {
        return Err(invalid("graph progress encoding"));
    }
    let p = Progress {
        watermark: (wm != -1).then_some(wm),
        idle: flags[0] != 0,
        eof: flags[1] != 0,
    };
    p.validate()?;
    Ok(p)
}
impl GraphCut {
    pub fn validate(&self) -> Result<()> {
        if self.micros < 0
            || self.observed_micros < 0
            || (self.sequence == 0 && (self.micros != 0 || self.ingested != 0))
            || self.sources.is_empty()
            || self.sources.len() > 16
            || self.unions.len() > 64
            || self.outputs.is_empty()
            || self.outputs.len() > 16
            || self.next_source >= self.sources.len()
            || self.idle_micros.is_some_and(|n| n <= 0)
        {
            return Err(invalid("invalid graph cut identity/count/time"));
        }
        for source in self.sources.values() {
            source.progress.validate()?;
            if source.position.identity.kind != "file"
                || source.last_input < 0
                || source.last_input > self.micros
                || source.contract > 2
                || (source.contract == 0 && source.progress.eof)
            {
                return Err(invalid("invalid File progress/time/contract"));
            }
        }
        for union in self.unions.values() {
            if !(2..=16).contains(&union.inputs.len()) {
                return Err(invalid("invalid Union input count"));
            }
            for input in &union.inputs {
                input.validate()?;
            }
            union.emitted.validate()?;
            if union.emitted.eof != union.inputs.iter().all(|p| p.eof)
                || union.emitted.idle != union.inputs.iter().all(|p| p.idle)
            {
                return Err(invalid("inconsistent Union activity"));
            }
        }
        if self.outputs.values().any(|n| *n == 0) {
            return Err(invalid("output ordinal is zero"));
        }
        // Also bound direct embedding literals, not only decoded GTC1 input.
        // GraphRuntime retains this object under a fixed metadata lease.
        let encoded_bytes = self
            .sources
            .values()
            .fold(64usize, |n, s| {
                n.saturating_add(67)
                    .saturating_add(s.position.identity.path.len())
            })
            .saturating_add(
                self.unions
                    .values()
                    .map(|u| 18 + 10 * u.inputs.len())
                    .sum::<usize>(),
            )
            .saturating_add(12 * self.outputs.len());
        if encoded_bytes > MAX_BYTES
            || self.ingested > self.sequence
            || self.ingested > self.records()?
        {
            return Err(invalid("graph cut metadata/record bound"));
        }
        Ok(())
    }
    pub fn records(&self) -> Result<u64> {
        self.sources.values().try_fold(0u64, |n, p| {
            n.checked_add(p.position.record_index)
                .ok_or_else(|| invalid("source record overflow"))
        })
    }
    pub fn wrap(&self) -> Result<SourcePosition> {
        self.validate()?;
        let mut raw = Vec::new();
        raw.extend_from_slice(b"GTC1");
        for n in [
            self.sequence,
            self.micros as u64,
            self.observed_micros as u64,
            self.ingested,
            self.next_source as u64,
            self.idle_micros.unwrap_or(-1) as u64,
        ] {
            put_u64(&mut raw, n);
        }
        raw.extend_from_slice(&(self.sources.len() as u32).to_le_bytes());
        for (&operator, s) in &self.sources {
            raw.extend_from_slice(&operator.to_le_bytes());
            if s.position.identity.path.len() > MAX_BYTES
                || raw
                    .len()
                    .saturating_add(s.position.identity.path.len() + 96)
                    > MAX_BYTES
            {
                return Err(invalid("source metadata exceeds graph cut bound"));
            }
            crate::checkpoint::encode_position(&s.position, &mut raw)?;
            put_progress(&mut raw, &s.progress);
            put_u64(&mut raw, s.last_input as u64);
            raw.push(s.contract);
        }
        raw.extend_from_slice(&(self.unions.len() as u32).to_le_bytes());
        for (&operator, u) in &self.unions {
            raw.extend_from_slice(&operator.to_le_bytes());
            raw.extend_from_slice(&(u.inputs.len() as u32).to_le_bytes());
            for p in &u.inputs {
                put_progress(&mut raw, p);
            }
            put_progress(&mut raw, &u.emitted);
        }
        raw.extend_from_slice(&(self.outputs.len() as u32).to_le_bytes());
        for (&operator, &n) in &self.outputs {
            raw.extend_from_slice(&operator.to_le_bytes());
            put_u64(&mut raw, n);
        }
        if raw.len() > MAX_BYTES {
            return Err(invalid("graph cut exceeds 30 KiB"));
        }
        let mut path = String::with_capacity(raw.len() * 2);
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for b in raw {
            path.push(HEX[(b >> 4) as usize] as char);
            path.push(HEX[(b & 15) as usize] as char);
        }
        Ok(SourcePosition {
            identity: SourceIdentity {
                kind: KIND.into(),
                path,
                size: 0,
                fingerprint: 0,
            },
            offset_bytes: 0,
            record_index: self.records()?,
        })
    }
    pub fn unwrap(position: &SourcePosition) -> Result<Self> {
        let hex = position.identity.path.as_bytes();
        if position.identity.kind != KIND
            || position.offset_bytes != 0
            || position.identity.size != 0
            || position.identity.fingerprint != 0
            || hex.len() > MAX_BYTES * 2
            || hex.len() % 2 != 0
        {
            return Err(invalid("graph source envelope"));
        }
        let digit = |c: u8| match c {
            b'0'..=b'9' => Ok(c - b'0'),
            b'a'..=b'f' => Ok(c - b'a' + 10),
            _ => Err(invalid("graph cut hex")),
        };
        let mut raw = Vec::with_capacity(hex.len() / 2);
        for pair in hex.chunks_exact(2) {
            raw.push(digit(pair[0])? * 16 + digit(pair[1])?);
        }
        let mut src = raw.as_slice();
        if take(&mut src, 4)? != b"GTC1" {
            return Err(invalid("graph cut magic"));
        }
        let sequence = number(&mut src)?;
        let micros = number(&mut src)? as i64;
        let observed_micros = number(&mut src)? as i64;
        let ingested = number(&mut src)?;
        let next_source =
            usize::try_from(number(&mut src)?).map_err(|_| invalid("source cursor overflow"))?;
        let idle = number(&mut src)? as i64;
        if idle < -1 {
            return Err(invalid("idle policy encoding"));
        }
        let mut sources = BTreeMap::new();
        for _ in 0..count(&mut src, 16)? {
            let operator = id(&mut src)?;
            let offset_bytes = number(&mut src)?;
            let record_index = number(&mut src)?;
            let kind = text(&mut src)?;
            let path = text(&mut src)?;
            let size = number(&mut src)?;
            let fingerprint = number(&mut src)?;
            let p = SourceProgress {
                position: SourcePosition {
                    offset_bytes,
                    record_index,
                    identity: SourceIdentity {
                        kind,
                        path,
                        size,
                        fingerprint,
                    },
                },
                progress: progress(&mut src)?,
                last_input: number(&mut src)? as i64,
                contract: take(&mut src, 1)?[0],
            };
            if sources.insert(operator, p).is_some() {
                return Err(invalid("duplicate graph source"));
            }
        }
        let mut unions = BTreeMap::new();
        for _ in 0..count(&mut src, 64)? {
            let operator = id(&mut src)?;
            let mut inputs = Vec::new();
            for _ in 0..count(&mut src, 16)? {
                inputs.push(progress(&mut src)?);
            }
            let u = UnionProgress {
                inputs,
                emitted: progress(&mut src)?,
            };
            if unions.insert(operator, u).is_some() {
                return Err(invalid("duplicate Union"));
            }
        }
        let mut outputs = BTreeMap::new();
        for _ in 0..count(&mut src, 16)? {
            if outputs.insert(id(&mut src)?, number(&mut src)?).is_some() {
                return Err(invalid("duplicate Sink"));
            }
        }
        let cut = Self {
            sequence,
            micros,
            observed_micros,
            ingested,
            next_source,
            idle_micros: (idle != -1).then_some(idle),
            sources,
            unions,
            outputs,
        };
        cut.validate()?;
        if !src.is_empty() || cut.records()? != position.record_index {
            return Err(invalid("graph cut trailing bytes/record mismatch"));
        }
        Ok(cut)
    }
    pub fn check_plan(&self, plan: &PhysicalPlan) -> Result<()> {
        self.validate()?;
        let manifest = CheckpointPlan::from_physical(plan)?;
        if !manifest.is_time_graph()
            || self.sources.keys().copied().collect::<Vec<_>>()
                != manifest
                    .source_ids()
                    .iter()
                    .map(|id| id.raw())
                    .collect::<Vec<_>>()
            || self.outputs.keys().copied().collect::<Vec<_>>()
                != manifest
                    .sink_ids()
                    .iter()
                    .map(|id| id.raw())
                    .collect::<Vec<_>>()
        {
            return Err(invalid("source/sink set differs from graph"));
        }
        let expected = plan
            .stages
            .iter()
            .enumerate()
            .filter_map(|(i, s)| match s {
                PhysicalStage::UnionAll { operator, .. } => Some((
                    operator.raw(),
                    plan.edges
                        .as_ref()
                        .unwrap()
                        .iter()
                        .filter(|e| e.to == i)
                        .count(),
                )),
                _ => None,
            })
            .collect::<BTreeMap<_, _>>();
        if expected.len() != self.unions.len()
            || expected
                .iter()
                .any(|(id, n)| self.unions.get(id).is_none_or(|p| p.inputs.len() != *n))
        {
            return Err(invalid("Union set/ports differ from graph"));
        }
        if !plan.recovery_event_time()
            && (self
                .sources
                .values()
                .any(|s| s.progress.watermark.is_some())
                || self.unions.values().any(|u| {
                    u.emitted.watermark.is_some() || u.inputs.iter().any(|p| p.watermark.is_some())
                }))
        {
            return Err(invalid("processing-time graph cannot carry ET progress"));
        }
        if plan.recovery_event_time() {
            for union in self.unions.values() {
                let required = if union.emitted.eof {
                    Some(i64::MAX)
                } else if !union.emitted.idle
                    && !union
                        .inputs
                        .iter()
                        .any(|p| !p.idle && p.watermark.is_none())
                {
                    union
                        .inputs
                        .iter()
                        .filter(|p| !p.idle)
                        .filter_map(|p| p.watermark)
                        .min()
                } else {
                    None
                };
                if required.is_some_and(|floor| union.emitted.watermark.is_none_or(|wm| wm < floor))
                {
                    return Err(invalid(
                        "committed Union watermark is below its input/EOF floor",
                    ));
                }
            }
        }
        // A committed cut is after every port's round and barrier. Window
        // operators transform watermark values but not idle/permanent EOF.
        // Pending decisions deliberately use `validate`, NOT this check: they
        // contain new source observations and the previous runtime Union cut.
        let mut activity = BTreeMap::new();
        let edges = plan.edges.as_ref().expect("time graph edges");
        for _ in 0..plan.stages.len() {
            let mut changed = false;
            for (index, stage) in plan.stages.iter().enumerate() {
                if activity.contains_key(&index) {
                    continue;
                }
                let incoming = edges.iter().filter(|e| e.to == index).collect::<Vec<_>>();
                let value = match stage {
                    PhysicalStage::MemorySource { operator, .. } => {
                        let p = &self.sources[&operator.raw()].progress;
                        (p.idle, p.eof)
                    }
                    _ if incoming.iter().any(|e| !activity.contains_key(&e.from)) => continue,
                    PhysicalStage::UnionAll { operator, .. } => {
                        let union = &self.unions[&operator.raw()];
                        for (edge, p) in incoming.iter().zip(&union.inputs) {
                            if activity[&edge.from] != (p.idle, p.eof) {
                                return Err(invalid(
                                    "Union activity differs from committed upstream cut",
                                ));
                            }
                        }
                        (union.emitted.idle, union.emitted.eof)
                    }
                    PhysicalStage::Analysis { plan, .. } if plan.is_join() => {
                        let eof = incoming.iter().all(|e| activity[&e.from].1);
                        (eof, eof)
                    }
                    _ => *activity
                        .get(&incoming[0].from)
                        .ok_or_else(|| invalid("disconnected time graph stage"))?,
                };
                activity.insert(index, value);
                changed = true;
            }
            if !changed {
                break;
            }
        }
        if activity.len() != plan.stages.len() {
            return Err(invalid("incomplete graph activity cut"));
        }
        Ok(())
    }
}

pub fn output_sequence(generation: [u8; 16], sink: u32, next: u64) -> Result<OutputSequence> {
    let mut epoch = generation;
    for (a, b) in epoch[..4].iter_mut().zip(sink.to_le_bytes()) {
        *a ^= b;
    }
    epoch[15] ^= 0xd8;
    OutputSequence::new(epoch, next)
}

/// Every Union records before forwarding its barrier; each required Sink ACK
/// records after real flush. The source reads this only after all ACKs arrive.
pub struct GraphRuntime {
    pub initial: GraphCut,
    pub event_time: bool,
    pub generation: [u8; 16],
    semantics: Vec<u8>,
    unions: Mutex<BTreeMap<u32, (u64, UnionProgress)>>,
    outputs: Mutex<BTreeMap<u32, (u64, u64)>>,
    observed: std::sync::atomic::AtomicI64,
    decision: Mutex<(u64, i64, bool)>,
    _credit: MemoryLease,
}
impl GraphRuntime {
    pub fn new(
        initial: GraphCut,
        plan: &PhysicalPlan,
        generation: [u8; 16],
        owner: &Arc<MemoryOwner>,
    ) -> Result<Arc<Self>> {
        initial.check_plan(plan)?;
        if generation == [0; 16] {
            return Err(invalid("missing generation"));
        }
        let credit = owner.acquire(CreditKind::Reservation, MAX_BYTES * 8)?;
        let sequence = initial.sequence;
        let micros = initial.micros;
        let semantics = CheckpointPlan::from_physical(plan)?.semantics;
        let observed = std::sync::atomic::AtomicI64::new(initial.observed_micros);
        Ok(Arc::new(Self {
            initial,
            event_time: plan.recovery_event_time(),
            generation,
            semantics,
            unions: Mutex::new(BTreeMap::new()),
            outputs: Mutex::new(BTreeMap::new()),
            observed,
            decision: Mutex::new((sequence, micros, false)),
            _credit: credit,
        }))
    }
    pub fn begin_round(&self, cut: &GraphCut) -> Result<()> {
        cut.validate()?;
        let mut decision = self.decision.lock().expect("graph round");
        if decision.2
            || decision.0.checked_add(1) != Some(cut.sequence)
            || cut.micros < decision.1
            || cut.sources.keys().ne(self.initial.sources.keys())
            || cut.unions.keys().ne(self.initial.unions.keys())
            || cut.outputs.keys().ne(self.initial.outputs.keys())
            || cut.idle_micros != self.initial.idle_micros
        {
            return Err(invalid("graph decisions must commit serially"));
        }
        *decision = (cut.sequence, cut.micros, true);
        self.observed
            .store(cut.observed_micros, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
    pub fn observed_micros(&self) -> i64 {
        self.observed.load(std::sync::atomic::Ordering::SeqCst)
    }
    pub fn belongs_to(&self, owner: &Arc<MemoryOwner>) -> bool {
        Arc::ptr_eq(self._credit.owner(), owner)
    }
    pub fn check_plan(&self, plan: &PhysicalPlan) -> Result<()> {
        self.initial.check_plan(plan)?;
        if self.semantics != CheckpointPlan::from_physical(plan)?.semantics {
            return Err(invalid("runtime graph semantics changed"));
        }
        Ok(())
    }
    pub fn source_kind(&self) -> &'static str {
        KIND
    }
    pub fn sink_sequence(&self, sink: u32) -> Result<OutputSequence> {
        output_sequence(
            self.generation,
            sink,
            *self
                .initial
                .outputs
                .get(&sink)
                .ok_or_else(|| invalid("unknown graph Sink"))?,
        )
    }
    pub fn record_union(&self, id: u32, checkpoint: u64, state: UnionProgress) -> Result<()> {
        if self
            .initial
            .unions
            .get(&id)
            .is_none_or(|u| u.inputs.len() != state.inputs.len())
        {
            return Err(invalid("unknown Union ACK"));
        }
        let mut all = self.unions.lock().expect("graph controls");
        if all.get(&id).is_some_and(|(old, value)| {
            *old > checkpoint || (*old == checkpoint && *value != state)
        }) {
            return Err(invalid("conflicting Union ACK"));
        }
        all.insert(id, (checkpoint, state));
        Ok(())
    }
    pub fn record_sink(&self, id: u32, checkpoint: u64, position: OutputSequence) -> Result<()> {
        let initial = self.sink_sequence(id)?;
        if initial.epoch() != position.epoch() || initial.first() > position.first() {
            return Err(invalid("invalid graph Sink ordinal"));
        }
        let mut all = self.outputs.lock().expect("graph outputs");
        if all.get(&id).is_some_and(|(old, n)| {
            *old > checkpoint
                || *n > position.first()
                || (*old == checkpoint && *n != position.first())
        }) {
            return Err(invalid("conflicting Sink ACK"));
        }
        all.insert(id, (checkpoint, position.first()));
        Ok(())
    }
    pub fn complete(&self, id: u64, cut: &mut GraphCut) -> Result<()> {
        let unions = self.unions.lock().expect("graph controls");
        let outputs = self.outputs.lock().expect("graph outputs");
        if unions.len() != cut.unions.len() || outputs.len() != cut.outputs.len() {
            return Err(invalid("missing graph control/output ACK"));
        }
        for (operator, state) in &mut cut.unions {
            let (checkpoint, next) = unions
                .get(operator)
                .ok_or_else(|| invalid("missing Union ACK"))?;
            if *checkpoint != id {
                return Err(invalid("stale Union ACK"));
            }
            *state = next.clone();
        }
        for (operator, next) in &mut cut.outputs {
            let (checkpoint, n) = outputs
                .get(operator)
                .ok_or_else(|| invalid("missing Sink ACK"))?;
            if *checkpoint != id {
                return Err(invalid("stale Sink ACK"));
            }
            *next = *n;
        }
        cut.validate()?;
        let mut decision = self.decision.lock().expect("graph round");
        if decision.0 != cut.sequence || decision.1 != cut.micros {
            return Err(invalid("graph control cut sequence mismatch"));
        }
        decision.2 = false;
        Ok(())
    }
}
