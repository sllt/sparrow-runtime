//! Opt-in static workbench under `/ui/` (K5.1). The directory is scanned once
//! at startup into an in-memory manifest: only regular files (no symlinks,
//! no hidden entries), bounded count/bytes. Requests are looked up by exact
//! manifest key, so traversal, directory listing and link following are
//! impossible. SPA fallback applies only to extension-less paths under /ui/.
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::Path as UrlPath;
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use sparrow_model::{ErrorCode, SparrowError};

const MAX_FILES: usize = 2048;
const MAX_TOTAL: u64 = 64 * 1024 * 1024;
const MAX_DEPTH: usize = 8;

pub const CSP: &str = "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; \
img-src 'self' data:; font-src 'self'; connect-src 'self'; object-src 'none'; base-uri 'none'; \
frame-ancestors 'none'; form-action 'self'";

#[derive(Default)]
pub struct UiAssets {
    files: HashMap<String, Arc<[u8]>>,
}

fn err(msg: impl Into<String>) -> SparrowError {
    SparrowError::new(ErrorCode::InvalidArgument, format!("--ui-dir: {}", msg.into()))
}

fn safe_segment(s: &str) -> bool {
    !s.is_empty()
        && !s.starts_with('.')
        && s.bytes().all(|b| b.is_ascii_alphanumeric() || b"-_.@~".contains(&b))
}

impl UiAssets {
    pub fn load(root: &Path) -> Result<Self, SparrowError> {
        let meta = std::fs::symlink_metadata(root).map_err(|e| err(format!("{}", e.kind())))?;
        if !meta.is_dir() {
            return Err(err("not a directory (symlinked roots are refused)"));
        }
        let mut out = UiAssets::default();
        let mut total = 0u64;
        let mut stack = vec![(root.to_path_buf(), String::new(), 0usize)];
        while let Some((dir, prefix, depth)) = stack.pop() {
            if depth > MAX_DEPTH {
                return Err(err("directory nesting too deep"));
            }
            for entry in std::fs::read_dir(&dir).map_err(|e| err(format!("{}", e.kind())))? {
                let entry = entry.map_err(|e| err(format!("{}", e.kind())))?;
                let name = entry.file_name();
                let Some(name) = name.to_str() else { continue };
                if !safe_segment(name) {
                    continue; // hidden or unusual names are never served
                }
                let ft = entry.file_type().map_err(|e| err(format!("{}", e.kind())))?;
                let key = if prefix.is_empty() { name.to_string() } else { format!("{prefix}/{name}") };
                if ft.is_symlink() {
                    continue;
                } else if ft.is_dir() {
                    stack.push((entry.path(), key, depth + 1));
                } else if ft.is_file() {
                    let bytes = std::fs::read(entry.path()).map_err(|e| err(format!("{}", e.kind())))?;
                    total += bytes.len() as u64;
                    if out.files.len() >= MAX_FILES || total > MAX_TOTAL {
                        return Err(err("asset count/bytes exceed limits"));
                    }
                    out.files.insert(key, bytes.into());
                }
            }
        }
        if !out.files.contains_key("index.html") {
            return Err(err("index.html missing"));
        }
        Ok(out)
    }

    pub fn from_files(files: impl IntoIterator<Item = (String, Vec<u8>)>) -> Self {
        UiAssets { files: files.into_iter().map(|(k, v)| (k, v.into())).collect() }
    }
}

fn content_type(path: &str) -> &'static str {
    match path.rsplit('.').next().unwrap_or("") {
        "html" => "text/html; charset=utf-8",
        "js" | "mjs" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "json" => "application/json",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "ico" => "image/x-icon",
        "woff2" => "font/woff2",
        "woff" => "font/woff",
        "txt" => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}

pub fn security_headers(resp: &mut Response) {
    let h = resp.headers_mut();
    h.insert("content-security-policy", HeaderValue::from_static(CSP));
    h.insert("x-content-type-options", HeaderValue::from_static("nosniff"));
    h.insert("x-frame-options", HeaderValue::from_static("DENY"));
    h.insert("referrer-policy", HeaderValue::from_static("no-referrer"));
    h.insert("cross-origin-opener-policy", HeaderValue::from_static("same-origin"));
}

fn serve(assets: &UiAssets, key: &str) -> Response {
    let segments_ok = key.is_empty() || key.split('/').all(safe_segment);
    let (key, found) = match segments_ok.then(|| assets.files.get(key)).flatten() {
        Some(b) => (key, Some(b.clone())),
        None => {
            let last = key.rsplit('/').next().unwrap_or("");
            if segments_ok && !last.contains('.') {
                ("index.html", assets.files.get("index.html").cloned())
            } else {
                (key, None)
            }
        }
    };
    let mut resp = match found {
        Some(bytes) => {
            let cache = if key.starts_with("assets/") {
                "public, max-age=31536000, immutable"
            } else {
                "no-cache"
            };
            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, content_type(key)), (header::CACHE_CONTROL, cache)],
                Body::from(bytes.to_vec()),
            )
                .into_response()
        }
        None => (
            StatusCode::NOT_FOUND,
            [(header::CONTENT_TYPE, "text/plain; charset=utf-8"), (header::CACHE_CONTROL, "no-store")],
            "not found",
        )
            .into_response(),
    };
    security_headers(&mut resp);
    resp
}

pub fn router<S: Clone + Send + Sync + 'static>(assets: Arc<UiAssets>) -> Router<S> {
    let a1 = assets.clone();
    let a2 = assets;
    Router::new()
        .route(
            "/ui",
            get(|| async {
                let mut r = (StatusCode::PERMANENT_REDIRECT, [(header::LOCATION, "/ui/")]).into_response();
                security_headers(&mut r);
                r
            }),
        )
        .route("/ui/", get(move || { let a = a1.clone(); async move { serve(&a, "") } }))
        .route(
            "/ui/{*path}",
            get(move |UrlPath(p): UrlPath<String>| { let a = a2.clone(); async move { serve(&a, &p) } }),
        )
}
