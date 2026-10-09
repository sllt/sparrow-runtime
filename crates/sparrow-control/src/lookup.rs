//! Managed Lookup configuration. Transport stays outside the core runtime.
use serde::{Deserialize, Serialize};
use sparrow_model::{ErrorCode, Result, Row, Scalar, Schema, SparrowError};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

/// A remote Lookup binding: exactly one of `url` (HTTP, one key per request)
/// or `redis` (GET/HMGET, pipelined batches) selects the provider. Both
/// implement the same runtime `ExternalLookup` trait and share the operator,
/// cache, options and error policy.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalLookupSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub header_secret: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redis: Option<RedisLookupSpec>,
    pub fields: Vec<sparrow_plan::graph::FieldSpec>,
    pub keys: Vec<String>,
    #[serde(default)]
    pub options: sparrow_runtime::external_lookup::ExternalLookupOptions,
}

/// Redis provider of an external Lookup. `options.max_inflight` is also the
/// connection pool size and `options.timeout_ms` the per-request deadline.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedisLookupSpec {
    /// `redis://host[:port][/db]` or `rediss://...` (TLS).
    pub url: String,
    /// Key template over the lookup key columns, e.g. `limits:{site}:{device}`.
    pub key: String,
    /// `hash` (HMGET of the non-key fields; default) or `json` (GET of a
    /// flat JSON object).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    /// ACL user name secret reference (requires `password_secret`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username_secret: Option<String>,
    /// Password secret reference (requires `rediss://`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password_secret: Option<String>,
    /// PEM bundle that replaces the built-in roots (verification stays on).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_pem: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connect_timeout_ms: Option<u64>,
}

impl ExternalLookupSpec {
    pub fn validate(&self, name: &str) -> Result<()> {
        if self.fields.len() > 16
            || self.keys.len() > 8
            || self.url.as_ref().is_some_and(|u| u.len() > 2048)
            || self
                .header_secret
                .as_ref()
                .is_some_and(|s| s.is_empty() || s.len() > 128)
        {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                "external Lookup metadata bound exceeded",
            ));
        }
        self.empty_table().validate()?;
        self.options.validate()?;
        self.schema(name)?;
        match (&self.url, &self.redis) {
            (Some(_), None) => {
                if self.options.batch_keys != 1 {
                    return Err(SparrowError::new(
                        ErrorCode::InvalidArgument,
                        "HTTP external Lookup sends one key per request; options.batch_keys must be 1",
                    ));
                }
            }
            (None, Some(_)) => {
                if self.header_secret.is_some() {
                    return Err(SparrowError::new(
                        ErrorCode::InvalidArgument,
                        "header_secret belongs to HTTP external Lookups; use redis.password_secret",
                    ));
                }
                sparrow_connectors::RedisLookup::check(&self.redis_config(name)?)?;
            }
            _ => {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "external Lookup needs exactly one provider: url (HTTP) or redis",
                ))
            }
        }
        Ok(())
    }
    fn empty_table(&self) -> crate::ReferenceTableSpec {
        crate::ReferenceTableSpec {
            fields: self.fields.clone(),
            keys: self.keys.clone(),
            rows: vec![],
        }
    }
    pub fn schema(&self, name: &str) -> Result<Schema> {
        self.empty_table().schema(name)
    }
    fn redis_config(&self, name: &str) -> Result<sparrow_connectors::RedisLookupConfig> {
        use sparrow_connectors::redis::{RedisLookupFormat, RedisTarget};
        let redis = self.redis.as_ref().ok_or_else(|| {
            SparrowError::new(ErrorCode::InvalidArgument, "redis external Lookup missing")
        })?;
        let format = match redis.format.as_deref() {
            None => RedisLookupFormat::Hash,
            Some(f) => RedisLookupFormat::parse(f).ok_or_else(|| {
                SparrowError::new(
                    ErrorCode::InvalidArgument,
                    "redis external Lookup format must be hash or json",
                )
            })?,
        };
        let mut target = RedisTarget::new(redis.url.clone());
        target.username_secret = redis.username_secret.clone();
        target.password_secret = redis.password_secret.clone();
        target.ca_pem = redis.ca_pem.clone();
        if let Some(ms) = redis.connect_timeout_ms {
            target.connect_timeout = Duration::from_millis(ms);
        }
        Ok(sparrow_connectors::RedisLookupConfig {
            target,
            schema: self.schema(name)?,
            keys: self.keys.clone(),
            key: redis.key.clone(),
            format,
            timeout: Duration::from_millis(self.options.timeout_ms),
            pool_size: self.options.max_inflight,
        })
    }
    pub(crate) fn provider(
        &self,
        name: &str,
        secrets: &dyn sparrow_connectors::SecretResolver,
        policy: &sparrow_connectors::TargetPolicy,
    ) -> Result<sparrow_runtime::external_lookup::ExternalLookupBinding> {
        self.validate(name)?;
        let provider: Arc<dyn sparrow_runtime::external_lookup::ExternalLookup> =
            if let Some(url) = &self.url {
                let config = sparrow_connectors::http_lookup::HttpLookupConfig {
                    url: url.clone(),
                    header_secret: self.header_secret.clone(),
                    schema: self.schema(name)?,
                    keys: self.keys.clone(),
                    timeout: Duration::from_millis(self.options.timeout_ms),
                };
                Arc::new(HttpProvider(
                    sparrow_connectors::http_lookup::HttpLookup::bind(config, secrets, policy)
                        .map_err(SparrowError::from)?,
                ))
            } else {
                Arc::new(RedisProvider(sparrow_connectors::RedisLookup::bind(
                    self.redis_config(name)?,
                    secrets,
                    policy,
                )?))
            };
        Ok(sparrow_runtime::external_lookup::ExternalLookupBinding {
            provider,
            options: self.options.clone(),
        })
    }
}
struct HttpProvider(sparrow_connectors::http_lookup::HttpLookup);
pub(crate) fn prepare_update(
    next: crate::ReferenceTableRow,
    old: &sparrow_runtime::ReferenceTable,
    owner: &Arc<sparrow_model::MemoryOwner>,
) -> Result<Arc<sparrow_runtime::ReferenceTable>> {
    if next.name != old.name
        || next.revision <= old.version
        || next.table.schema(&next.name)? != old.schema
        || next.table.keys != old.key_fields
    {
        return Err(SparrowError::new(
            ErrorCode::InvalidSchema,
            "live Lookup revision/schema/key contract changed",
        ));
    }
    let _scratch = owner.acquire(
        sparrow_model::CreditKind::Reservation,
        (next.payload_bytes as usize)
            .saturating_mul(64)
            .saturating_add(64 * 1024),
    )?;
    let digest = crate::validate::decode_reference_sha256(&next.sha256)?;
    sparrow_runtime::ReferenceTable::snapshot_owned_verified(
        next.name,
        next.revision,
        next.table.schema(&old.name)?,
        next.table.keys.clone(),
        next.table.rows()?,
        crate::MAX_REFERENCE_TABLE_ROWS,
        owner.budget().retention_bytes,
        digest,
        owner,
    )
}
impl sparrow_runtime::external_lookup::ExternalLookup for HttpProvider {
    fn schema(&self) -> &Schema {
        self.0.schema()
    }
    fn keys(&self) -> &[String] {
        self.0.keys()
    }
    fn scratch_bytes(&self) -> usize {
        self.0.scratch_bytes()
    }
    fn lookup<'a>(
        &'a self,
        key: Vec<Scalar>,
        cancel: CancellationToken,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Option<Row>>> + Send + 'a>> {
        Box::pin(async move { self.0.lookup(key, cancel).await.map_err(SparrowError::from) })
    }
}

struct RedisProvider(sparrow_connectors::RedisLookup);
impl sparrow_runtime::external_lookup::ExternalLookup for RedisProvider {
    fn schema(&self) -> &Schema {
        self.0.schema()
    }
    fn keys(&self) -> &[String] {
        self.0.keys()
    }
    fn scratch_bytes(&self) -> usize {
        self.0.scratch_bytes()
    }
    fn max_batch_keys(&self) -> usize {
        self.0.max_batch_keys()
    }
    fn lookup<'a>(
        &'a self,
        key: Vec<Scalar>,
        cancel: CancellationToken,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Option<Row>>> + Send + 'a>> {
        Box::pin(self.0.lookup(key, cancel))
    }
    fn lookup_batch<'a>(
        &'a self,
        keys: Vec<Vec<Scalar>>,
        cancel: CancellationToken,
    ) -> sparrow_runtime::external_lookup::LookupBatchFuture<'a> {
        Box::pin(self.0.lookup_batch(keys, cancel))
    }
}
