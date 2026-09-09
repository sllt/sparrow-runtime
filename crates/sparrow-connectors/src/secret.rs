use std::collections::HashMap;
use std::sync::Arc;

use sparrow_model::ErrorCode;

use crate::error::{ConnectorError, Result};

/// Resolves named secrets. Missing names fail closed (`SecretMissing`).
pub trait SecretResolver: Send + Sync {
    fn resolve(&self, name: &str) -> Result<String>;
}

/// `env:FOO` reads `FOO`; other names look up an in-memory map.
#[derive(Clone, Default)]
pub struct MapSecretResolver {
    values: Arc<HashMap<String, String>>,
}

impl MapSecretResolver {
    pub fn new(values: HashMap<String, String>) -> Self {
        Self {
            values: Arc::new(values),
        }
    }

    pub fn empty() -> Self {
        Self::default()
    }
}

impl SecretResolver for MapSecretResolver {
    fn resolve(&self, name: &str) -> Result<String> {
        if let Some(env_name) = name.strip_prefix("env:") {
            return std::env::var(env_name).map_err(|_| {
                ConnectorError::new(
                    ErrorCode::SecretMissing,
                    format!("environment secret `{env_name}` is not set"),
                )
            });
        }
        self.values.get(name).cloned().ok_or_else(|| {
            ConnectorError::new(
                ErrorCode::SecretMissing,
                format!("secret `{name}` is not configured"),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_secret_is_explicit() {
        let r = MapSecretResolver::empty();
        let err = r.resolve("mqtt.password").unwrap_err();
        assert_eq!(err.code(), ErrorCode::SecretMissing);
    }
}
