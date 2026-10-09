use super::connection::{error, MAX_MESSAGE_BYTES};
use async_nats::jetstream::stream::{Info, RetentionPolicy, StorageType};
use sparrow_model::{ErrorCode, Result};

/// The exact created timestamp (not a 64-bit lossy hash) survives restarts but
/// changes on stream recreation. namespace identifies the configured account.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamIdentity {
    pub namespace: String,
    pub stream: String,
    pub created_nanos: i128,
    reader_binding: Option<(String, String)>,
}
impl StreamIdentity {
    pub fn from_info(namespace: &str, info: &Info) -> Result<Self> {
        check_name(namespace)?;
        check_name(&info.config.name)?;
        Ok(Self {
            namespace: namespace.to_owned(),
            stream: info.config.name.clone(),
            created_nanos: info.created.unix_timestamp_nanos(),
            reader_binding: None,
        })
    }
    pub(super) fn with_reader(mut self, bucket: &str, consumer: &str) -> Self {
        self.reader_binding = Some((bucket.into(), consumer.into()));
        self
    }
    pub fn position(&self, sequence: u64, records: u64) -> sparrow_io::SourcePosition {
        self.position_bound(sequence, records, 0)
    }
    /// [`Self::position`] with the payload format bound into the identity
    /// fingerprint (see [`format_fingerprint`]; 0 for JSON, so existing JSON
    /// cuts are unchanged).
    pub fn position_bound(
        &self,
        sequence: u64,
        records: u64,
        format: u64,
    ) -> sparrow_io::SourcePosition {
        sparrow_io::SourcePosition {
            offset_bytes: sequence,
            record_index: records,
            identity: sparrow_io::SourceIdentity {
                kind: "jetstream-v1".into(),
                path: match &self.reader_binding {
                    Some((bucket, consumer)) => format!(
                        "{}:{}:{}:{bucket}:{consumer}",
                        self.namespace, self.stream, self.created_nanos
                    ),
                    None => format!("{}:{}:{}", self.namespace, self.stream, self.created_nanos),
                },
                size: 0,
                fingerprint: format,
            },
        }
    }
    pub fn check_position(&self, position: &sparrow_io::SourcePosition) -> Result<()> {
        self.check_position_bound(position, 0)
    }
    /// Strict in both directions: a JSON cut (fingerprint 0) is refused by a
    /// CSV reader, a CSV cut by a JSON reader or one with other CSV options.
    pub fn check_position_bound(
        &self,
        position: &sparrow_io::SourcePosition,
        format: u64,
    ) -> Result<()> {
        let live = self
            .position_bound(position.offset_bytes, position.record_index, format)
            .identity;
        if live.kind != position.identity.kind
            || live.path != position.identity.path
            || live.size != position.identity.size
        {
            return Err(error(
                ErrorCode::UnsupportedRestore,
                "JetStream account/stream generation differs from checkpoint",
            ));
        }
        if live.fingerprint != position.identity.fingerprint {
            return Err(error(
                ErrorCode::UnsupportedRestore,
                "JetStream checkpoint was taken with a different payload format / CSV options",
            ));
        }
        Ok(())
    }
}

/// Checkpoint fingerprint of a payload format: 0 for JSON (the identity every
/// existing JetStream cut carries), otherwise a non-zero hash of the
/// canonical CSV decode options and effective limits.
pub fn format_fingerprint(format: &sparrow_formats::PayloadFormat) -> u64 {
    match format.identity_bytes() {
        None => 0,
        Some(bytes) => sparrow_io::fnv1a64(&bytes).max(1),
    }
}

pub(super) fn check_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 128
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(error(
            ErrorCode::InvalidArgument,
            "JetStream names require 1..=128 ASCII alphanumeric, underscore or dash",
        ));
    }
    Ok(())
}

/// Conservative initial admission: a finite, file-backed Limits stream with
/// prefix-only expiry. No interior deletion, subject eviction, transformations,
/// mirrors or new message modes. Repeat on health ticks and before checkpoint/restore.
/// Broker admin is trusted not to change and revert policies between checks;
/// delivery sequences independently detect observed gaps in the reader.
pub fn check_stream(info: &Info, next_required: u64) -> Result<()> {
    let c = &info.config;
    check_name(&c.name)?;
    if c.storage != StorageType::File
        || c.retention != RetentionPolicy::Limits
        || c.no_ack
        || !c.deny_delete
        || !c.deny_purge
        || c.allow_rollup
        || c.max_messages_per_subject > 0
        || c.discard_new_per_subject
        || c.max_bytes <= 0
        || c.max_bytes > 64 * 1024 * 1024 * 1024
        || c.max_message_size <= 0
        || c.max_message_size as usize > MAX_MESSAGE_BYTES
        || c.max_consumers <= 0
        || c.max_consumers > 128
        || c.num_replicas != 1
        || c.mirror.is_some()
        || c.sources.is_some()
        || c.subject_transform.is_some()
        || c.republish.is_some()
        || !c.template_owner.is_empty()
        || c.allow_message_ttl
        || c.subject_delete_marker_ttl.is_some()
        || c.allow_message_schedules
        || c.allow_message_counter
        || c.allow_batch_publish
        || c.allow_atomic_publish
        || c.first_sequence.is_some_and(|n| n > 1)
        || c.subjects.is_empty()
        || c.subjects.len() > 32
        || c.subjects.iter().any(|s| s.len() > 256)
        || info.state.deleted_count.unwrap_or(0) != 0
    {
        return Err(error(
            ErrorCode::UnsupportedRestore,
            "JetStream stream policy outside verified single-node Limits/File profile",
        ));
    }
    if next_required == 0
        || next_required > info.state.last_sequence.saturating_add(1)
        || info.state.first_sequence > next_required
    {
        return Err(error(
            ErrorCode::UnsupportedRestore,
            "JetStream required replay range no longer retained; refusing to skip input",
        ));
    }
    Ok(())
}
