//! Detached, domain-separated Ed25519 approval of the canonical manifest.
//! Signatures prove authorship under an administrator's policy, not code safety.
use crate::{identifier, invalid, Manifest};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde::{Deserialize, Serialize};
use sparrow_model::{ErrorCode, Result, SparrowError};
use std::{io::Read, path::Path};
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Signature {
    pub algorithm: String,
    pub key_id: String,
    pub signature_base64: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Publisher {
    pub id: String,
    pub public_key_base64: String,
    pub packages: Vec<String>,
    pub kinds: Vec<String>,
    #[serde(default)]
    pub revoked: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrustPolicy {
    pub format: u32,
    pub require_signed: bool,
    pub publishers: Vec<Publisher>,
}
impl Default for TrustPolicy {
    fn default() -> Self {
        Self {
            format: 1,
            require_signed: false,
            publishers: vec![],
        }
    }
}
fn denied() -> SparrowError {
    SparrowError::new(
        ErrorCode::PolicyDenied,
        "plugin publisher signature is missing, invalid, revoked or outside policy",
    )
}
pub fn signing_message(manifest: &Manifest, key_id: &str) -> Result<Vec<u8>> {
    if !identifier(key_id) {
        return Err(invalid("invalid signing key id"));
    }
    let mut out = b"sparrow-package-signature-v1\0".to_vec();
    out.extend_from_slice(&(key_id.len() as u32).to_be_bytes());
    out.extend_from_slice(key_id.as_bytes());
    out.extend_from_slice(&manifest.bytes()?);
    Ok(out)
}
impl TrustPolicy {
    pub fn validate(&self) -> Result<()> {
        if self.format != 1 || self.publishers.len() > 16 {
            return Err(invalid("invalid plugin trust policy version/size"));
        }
        let mut ids = std::collections::BTreeSet::new();
        for key in &self.publishers {
            if !identifier(&key.id)
                || !ids.insert(&key.id)
                || key.public_key_base64.len() != 44
                || !STANDARD
                    .decode(&key.public_key_base64)
                    .is_ok_and(|v| v.len() == 32)
                || key.packages.is_empty()
                || key.packages.len() > 32
                || key.kinds.is_empty()
                || key.kinds.len() > 4
                || key.packages.iter().any(|p| p != "*" && !identifier(p))
                || key.kinds.iter().any(|k| {
                    !matches!(
                        k.as_str(),
                        "native_scalar" | "javascript_scalar" | "wasm_scalar" | "native_extension"
                    )
                })
            {
                return Err(invalid("invalid publisher key or grant"));
            }
        }
        Ok(())
    }
    pub fn from_file(path: &Path) -> Result<Self> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
            let file = std::fs::OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
                .open(path)
                .map_err(|_| invalid("plugin trust policy unavailable"))?;
            let meta = file
                .metadata()
                .map_err(|_| invalid("plugin trust policy unavailable"))?;
            if !meta.is_file() || meta.len() > 64 * 1024 {
                return Err(invalid("plugin trust policy file rejected"));
            }
            if meta.mode() & 0o022 != 0
                || (meta.uid() != 0 && meta.uid() != unsafe { libc::geteuid() })
            {
                return Err(invalid("plugin trust policy must be administrator/service-owned and not group/world writable"));
            }
            let mut bytes = Vec::new();
            file.take(64 * 1024 + 1)
                .read_to_end(&mut bytes)
                .map_err(|_| invalid("plugin trust policy read failed"))?;
            if bytes.len() > 64 * 1024 {
                return Err(invalid("plugin trust policy too large"));
            }
            let policy: Self = serde_json::from_slice(&bytes)
                .map_err(|_| invalid("invalid plugin trust policy JSON"))?;
            policy.validate()?;
            Ok(policy)
        }
        #[cfg(not(unix))]
        {
            Err(invalid("plugin trust policy requires Unix"))
        }
    }
    pub fn verify(&self, manifest: &Manifest, signature: Option<&Signature>) -> Result<()> {
        self.validate()?;
        let Some(signature) = signature else {
            return if self.require_signed {
                Err(denied())
            } else {
                Ok(())
            };
        };
        if signature.algorithm != "ed25519" || signature.signature_base64.len() != 88 {
            return Err(denied());
        }
        let publisher = self
            .publishers
            .iter()
            .find(|p| {
                p.id == signature.key_id
                    && !p.revoked
                    && p.packages.iter().any(|n| n == "*" || n == &manifest.name)
                    && p.kinds.contains(&manifest.kind)
            })
            .ok_or_else(denied)?;
        let key = STANDARD
            .decode(&publisher.public_key_base64)
            .map_err(|_| denied())?;
        let bytes = STANDARD
            .decode(&signature.signature_base64)
            .map_err(|_| denied())?;
        if bytes.len() != 64 {
            return Err(denied());
        }
        ring::signature::UnparsedPublicKey::new(&ring::signature::ED25519, &key)
            .verify(&signing_message(manifest, &signature.key_id)?, &bytes)
            .map_err(|_| denied())
    }
}
