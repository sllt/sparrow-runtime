use sparrow_model::ErrorCode;

use crate::error::{ConnectorError, Result};

/// TLS settings for a connector. Verification is mandatory whenever TLS is on.
/// There is no skip-verify default and `skip_verify = true` is rejected.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TlsConfig {
    pub enabled: bool,
    pub skip_verify: bool,
}

impl TlsConfig {
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            skip_verify: false,
        }
    }

    pub fn enabled() -> Self {
        Self {
            enabled: true,
            skip_verify: false,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.skip_verify {
            return Err(ConnectorError::new(
                ErrorCode::PolicyDenied,
                "TLS skip_verify is rejected; certificate verification is mandatory when TLS is used",
            ));
        }
        Ok(())
    }
}

/// HTTP client builder that never disables certificate verification.
pub fn http_client(timeout: std::time::Duration, connect_timeout: std::time::Duration) -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(timeout)
        .connect_timeout(connect_timeout)
        .tls_built_in_root_certs(true)
        .https_only(false)
        .build()
        .map_err(|e| ConnectorError::new(ErrorCode::Internal, format!("http client: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skip_verify_is_rejected() {
        let err = TlsConfig {
            enabled: true,
            skip_verify: true,
        }
        .validate()
        .unwrap_err();
        assert_eq!(err.code(), ErrorCode::PolicyDenied);
    }
}
