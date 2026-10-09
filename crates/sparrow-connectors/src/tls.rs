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
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .retry(reqwest::retry::never())
        .tls_built_in_root_certs(true)
        .https_only(false)
        .build()
        .map_err(|e| ConnectorError::new(ErrorCode::Internal, format!("http client: {e}")))
}

/// Largest accepted `ca_pem` bundle.
pub const MAX_CA_PEM_BYTES: usize = 64 * 1024;

/// HTTPS-only client for credentialed targets: certificate verification is
/// always on, redirects/proxies/retries are off. `ca_pem` (one or more PEM
/// certificates) replaces the built-in webpki roots.
pub fn https_client(
    timeout: std::time::Duration,
    connect_timeout: std::time::Duration,
    ca_pem: Option<&str>,
) -> Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .timeout(timeout)
        .connect_timeout(connect_timeout)
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .retry(reqwest::retry::never())
        .https_only(true);
    match ca_pem {
        None => builder = builder.tls_built_in_root_certs(true),
        Some(pem) => {
            if pem.len() > MAX_CA_PEM_BYTES {
                return Err(ConnectorError::new(
                    ErrorCode::BoundExceeded,
                    "ca_pem exceeds 65536 bytes",
                ));
            }
            let certs = reqwest::Certificate::from_pem_bundle(pem.as_bytes()).map_err(|_| {
                ConnectorError::new(
                    ErrorCode::InvalidArgument,
                    "ca_pem is not a PEM certificate bundle",
                )
            })?;
            if certs.is_empty() {
                return Err(ConnectorError::new(
                    ErrorCode::InvalidArgument,
                    "ca_pem contains no certificate",
                ));
            }
            builder = builder.tls_built_in_root_certs(false);
            for cert in certs {
                builder = builder.add_root_certificate(cert);
            }
        }
    }
    builder
        .build()
        .map_err(|e| ConnectorError::new(ErrorCode::Internal, format!("https client: {e}")))
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

    #[tokio::test]
    async fn v01_http_client_does_not_follow_redirects() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            if let Ok((mut s, _)) = listener.accept().await {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut buf = [0u8; 256];
                let _ = s.read(&mut buf).await;
                let body = b"HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:1/secret\r\nContent-Length: 0\r\n\r\n";
                let _ = s.write_all(body).await;
            }
        });
        let client = http_client(std::time::Duration::from_secs(2), std::time::Duration::from_secs(1)).unwrap();
        let resp = client
            .get(format!("http://127.0.0.1:{port}/"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status().as_u16(),
            302,
            "auto-redirect must be disabled; got {}",
            resp.status()
        );
    }
}
