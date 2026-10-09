//! Managed Lookup configuration. Transport stays outside the core runtime.
use serde::{Deserialize, Serialize};
use sparrow_model::{ErrorCode, Result, Row, Scalar, Schema, SparrowError};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalLookupSpec {
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub header_secret: Option<String>,
    pub fields: Vec<sparrow_plan::graph::FieldSpec>,
    pub keys: Vec<String>,
    #[serde(default)]
    pub options: sparrow_runtime::external_lookup::ExternalLookupOptions,
}
impl ExternalLookupSpec {
    pub fn validate(&self, name: &str) -> Result<()> {
        if self.fields.len() > 16
            || self.keys.len() > 8
            || self.url.len() > 2048
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
    pub(crate) fn provider(
        &self,
        name: &str,
        secrets: &dyn sparrow_connectors::SecretResolver,
        policy: &sparrow_connectors::TargetPolicy,
    ) -> Result<sparrow_runtime::external_lookup::ExternalLookupBinding> {
        self.validate(name)?;
        let config = sparrow_connectors::http_lookup::HttpLookupConfig {
            url: self.url.clone(),
            header_secret: self.header_secret.clone(),
            schema: self.schema(name)?,
            keys: self.keys.clone(),
            timeout: Duration::from_millis(self.options.timeout_ms),
        };
        let provider = sparrow_connectors::http_lookup::HttpLookup::bind(config, secrets, policy)
            .map_err(SparrowError::from)?;
        Ok(sparrow_runtime::external_lookup::ExternalLookupBinding {
            provider: Arc::new(HttpProvider(provider)),
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
