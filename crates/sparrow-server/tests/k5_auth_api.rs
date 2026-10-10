//! K5.1 server-side RBAC, viewer-safe projection, `/v1/auth/me`, no-store
//! and the opt-in `/ui/` static surface. Direct API negative tests per role.
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use sparrow_control::{compact_kernel, PipelineSpec, Store, Supervisor};
use sparrow_server::{router, router_with_ui, AppState, Auth, UiAssets};
use tower::ServiceExt;

const VIEWER: &str = "k5-viewer-token-AAAA";
const OPERATOR: &str = "k5-operator-token-BBBB";
const ADMIN: &str = "k5-admin-token-CCCC";
const SENTINEL_SQL: &str = "SENTINEL_SQL_LITERAL_7781";
const SENTINEL_PATH: &str = "sentinel-path-9931";

fn auth() -> Auth {
    let h = |t: &str| sparrow_plugin::sha256(t.as_bytes());
    let doc = json!({"version":1,"principals":[
        {"actor":"vera","role":"viewer","token_sha256":h(VIEWER)},
        {"actor":"otto","role":"operator","token_sha256":h(OPERATOR)},
        {"actor":"ada","role":"admin","token_sha256":h(ADMIN)},
    ]});
    Auth::from_json(doc.to_string().as_bytes()).unwrap()
}

fn setup() -> AppState {
    let store = Arc::new(Store::open_memory().unwrap());
    let kernel = Arc::new(compact_kernel().unwrap());
    let supervisor = Supervisor::new(store.clone(), kernel, false, None).unwrap();
    let spec: PipelineSpec = serde_json::from_value(json!({
        "version": 1, "stream": "events",
        "sql": format!("SELECT id FROM events WHERE id = '{SENTINEL_SQL}'"),
        "source": {"kind": "file", "path": format!("{SENTINEL_PATH}.ndjson")},
        "sink": {"kind": "log"},
        "delivery": "live_best_effort", "recovery": "restart_fresh"
    }))
    .unwrap();
    store.put_pipeline("p1", &spec, None).unwrap();
    AppState { store, supervisor, token: Arc::new(auth()), safe_mode: false }
}

async fn call_on(app: axum::Router, method: Method, uri: &str, token: Option<&str>, body: Value) -> (StatusCode, axum::http::HeaderMap, String) {
    let mut b = Request::builder().method(method).uri(uri).header("content-type", "application/json");
    if let Some(t) = token {
        b = b.header("authorization", format!("Bearer {t}"));
    }
    let body = if body.is_null() { Body::empty() } else { Body::from(body.to_string()) };
    let r = app.oneshot(b.body(body).unwrap()).await.unwrap();
    let (status, headers) = (r.status(), r.headers().clone());
    let bytes = r.into_body().collect().await.unwrap().to_bytes();
    (status, headers, String::from_utf8_lossy(&bytes).into_owned())
}

async fn call(s: &AppState, m: Method, uri: &str, t: Option<&str>) -> (StatusCode, axum::http::HeaderMap, String) {
    call_on(router(s.clone()), m, uri, t, Value::Null).await
}

/// Every mutating/sensitive route a viewer and operator must not reach.
const ADMIN_ONLY: &[(&str, &str)] = &[
    ("PUT", "/v1/secrets/s1"), ("PUT", "/v1/allowlist"), ("POST", "/v1/pipelines/p1/kill"),
    ("POST", "/v1/pipelines/p1/restore"), ("POST", "/v1/pipelines/p1/retire"),
    ("POST", "/v1/plugins/install"), ("POST", "/v1/plugins/abc/enable"), ("POST", "/v1/plugins/abc/disable"),
    ("POST", "/v1/plugins/abc/uninstall"), ("POST", "/v1/plugins/abc/attest"),
    ("POST", "/v1/tables/t/gc"), ("POST", "/v1/tables/t/rollback"),
    ("GET", "/v1/pipelines/p1/outbox/entries"), ("GET", "/v1/pipelines/p1/outbox/entries/1"),
    ("POST", "/v1/pipelines/p1/outbox/command"), ("GET", "/v1/pipelines/p1/input-dlq/entries"),
    ("GET", "/v1/pipelines/p1/input-dlq/entries/1"), ("POST", "/v1/pipelines/p1/input-dlq/purge"),
    ("POST", "/v1/pipelines/p1/recovery/preview"), ("POST", "/v1/pipelines/p1/recovery/execute"),
    ("POST", "/v1/pipelines/p1/recovery/operations/x/finish"), ("POST", "/v1/pipelines/p1/recovery/operations/x/abort"),
    ("DELETE", "/v1/pipelines/p1"), ("GET", "/v1/unknown-route"),
];
const OPERATOR_ONLY: &[(&str, &str)] = &[
    ("POST", "/v1/validate"), ("POST", "/v1/explain"), ("POST", "/v1/graphs/validate"), ("POST", "/v1/graphs/explain"),
    ("POST", "/v1/test"), ("POST", "/v1/query"), ("GET", "/v1/streams"), ("GET", "/v1/streams/events"),
    ("PUT", "/v1/streams/events"), ("GET", "/v1/tables"), ("PUT", "/v1/tables/t"), ("POST", "/v1/tables/t/mutate"),
    ("GET", "/v1/pipelines/p1"), ("PUT", "/v1/pipelines/p1"), ("POST", "/v1/pipelines/p1/start"),
    ("POST", "/v1/pipelines/p1/stop"), ("POST", "/v1/pipelines/p1/checkpoint"), ("GET", "/v1/pipelines/p1/checkpoints"),
    ("GET", "/v1/pipelines/p1/outbox"), ("GET", "/v1/pipelines/p1/input-dlq"), ("GET", "/v1/plugins"),
];
const VIEWER_OK: &[&str] = &[
    "/v1/auth/me", "/v1/capabilities", "/v1/metrics", "/v1/pipelines", "/v1/pipelines/p1/status",
    "/v1/pipelines/p1/diagnose", "/v1/audit",
];

#[tokio::test]
async fn k5_rbac_negative_matrix_per_role() {
    let s = setup();
    let audit_before = s.store.list_audit(50).unwrap().len();
    for (m, uri) in ADMIN_ONLY.iter().chain(OPERATOR_ONLY) {
        let (st, h, body) = call(&s, m.parse().unwrap(), uri, Some(VIEWER)).await;
        assert_eq!(st, StatusCode::FORBIDDEN, "viewer {m} {uri}: {body}");
        assert_eq!(h["cache-control"], "no-store");
        assert!(body.contains("forbidden: role viewer"), "{body}");
    }
    for (m, uri) in ADMIN_ONLY {
        let (st, _, body) = call(&s, m.parse().unwrap(), uri, Some(OPERATOR)).await;
        assert_eq!(st, StatusCode::FORBIDDEN, "operator {m} {uri}: {body}");
    }
    // 403s never write audit rows
    assert_eq!(s.store.list_audit(50).unwrap().len(), audit_before);
    // Operator/admin pass authorization (handler may still reject the empty body).
    for (m, uri) in OPERATOR_ONLY {
        for t in [OPERATOR, ADMIN] {
            let (st, _, body) = call(&s, m.parse().unwrap(), uri, Some(t)).await;
            assert!(st != StatusCode::FORBIDDEN && st != StatusCode::UNAUTHORIZED, "{t} {m} {uri}: {st} {body}");
        }
    }
    for (m, uri) in ADMIN_ONLY.iter().filter(|(_, u)| *u != "/v1/unknown-route" && !u.starts_with("/v1/plugins")) {
        let (st, _, body) = call(&s, m.parse().unwrap(), uri, Some(ADMIN)).await;
        assert!(st != StatusCode::FORBIDDEN && st != StatusCode::UNAUTHORIZED, "admin {m} {uri}: {st} {body}");
    }
    for uri in VIEWER_OK {
        let (st, h, body) = call(&s, Method::GET, uri, Some(VIEWER)).await;
        assert_eq!(st, StatusCode::OK, "viewer {uri}: {body}");
        assert_eq!(h["cache-control"], "no-store");
    }
    // Unauthenticated and wrong token: 401 everywhere except / and health.
    let audit_before = s.store.list_audit(50).unwrap().len();
    for t in [None, Some("wrong"), Some(""), Some("k5-viewer-token-AAA")] {
        let (st, h, _) = call(&s, Method::GET, "/v1/pipelines/p1/status", t).await;
        assert_eq!(st, StatusCode::UNAUTHORIZED);
        assert_eq!(h["cache-control"], "no-store");
    }
    assert_eq!(call(&s, Method::GET, "/v1/health", None).await.0, StatusCode::OK);
    assert_eq!(call(&s, Method::GET, "/", None).await.0, StatusCode::OK);
    // auth failures never write audit rows
    assert_eq!(s.store.list_audit(50).unwrap().len(), audit_before);
}

#[tokio::test]
async fn k5_viewer_projection_and_audit_do_not_leak() {
    let s = setup();
    let (_, _, op) = call(&s, Method::GET, "/v1/pipelines/p1/status", Some(OPERATOR)).await;
    assert!(op.contains(SENTINEL_SQL), "operator sees full spec");
    // operator validate must not audit SQL literals
    let spec = json!({"version":1,"stream":"events","sql":format!("SELECT '{SENTINEL_SQL}' FROM events"),
        "source":{"kind":"file","path":"x.ndjson"},"sink":{"kind":"log"}});
    let _ = call_on(router(s.clone()), Method::POST, "/v1/validate", Some(OPERATOR), spec).await;
    s.store.audit("otto", "put_pipeline", Some("p1"), Some(&format!("detail-{SENTINEL_PATH}")), "ok").unwrap();
    for uri in VIEWER_OK {
        let (st, _, body) = call(&s, Method::GET, uri, Some(VIEWER)).await;
        assert_eq!(st, StatusCode::OK);
        for needle in [SENTINEL_SQL, SENTINEL_PATH, VIEWER, OPERATOR, ADMIN, "token_sha256", "\"spec\""] {
            assert!(!body.contains(needle), "viewer {uri} leaked {needle}: {body}");
        }
    }
    let (_, _, st) = call(&s, Method::GET, "/v1/pipelines/p1/status", Some(VIEWER)).await;
    let v: Value = serde_json::from_str(&st).unwrap();
    assert_eq!(v["projection"], "viewer_safe");
    for row in s.store.list_audit(50).unwrap() {
        let all = format!("{:?}{:?}{:?}", row.target, row.detail, row.actor);
        assert!(!all.contains(SENTINEL_SQL), "audit kept SQL literal: {all}");
        for t in [VIEWER, OPERATOR, ADMIN] { assert!(!all.contains(t)); }
    }
}

#[tokio::test]
async fn k5_auth_me_and_real_actor_in_audit() {
    let s = setup();
    let (_, _, me) = call(&s, Method::GET, "/v1/auth/me", Some(OPERATOR)).await;
    let me: Value = serde_json::from_str(&me).unwrap();
    assert_eq!(me["actor"], "otto");
    assert_eq!(me["role"], "operator");
    assert_eq!(me["auth_mode"], "auth_file");
    let acts = me["allowed_actions"].as_array().unwrap();
    assert!(acts.contains(&json!("pipeline.start")) && !acts.contains(&json!("secret.write")));
    let (st, _, body) = call_on(router(s.clone()), Method::PUT, "/v1/secrets/k5s", Some(ADMIN), json!({"value":"hunter2-SECRET"})).await;
    assert_eq!(st, StatusCode::NO_CONTENT, "{body}");
    let rows = s.store.list_audit(10).unwrap();
    let row = rows.iter().find(|r| r.action == "put_secret").unwrap();
    assert_eq!(row.actor, "ada");
    assert!(!format!("{:?}", rows.iter().map(|r| (&r.target, &r.detail)).collect::<Vec<_>>()).contains("hunter2"));
    let (_, _, aud) = call(&s, Method::GET, "/v1/audit", Some(ADMIN)).await;
    assert!(!aud.contains("hunter2"));
}

#[tokio::test]
async fn k5_legacy_single_token_is_admin() {
    let mut s = setup();
    s.token = Arc::new("legacy-admin-token-0001".into());
    let (_, _, me) = call(&s, Method::GET, "/v1/auth/me", Some("legacy-admin-token-0001")).await;
    let me: Value = serde_json::from_str(&me).unwrap();
    assert_eq!((me["role"].as_str(), me["auth_mode"].as_str()), (Some("admin"), Some("legacy_single_token")));
    assert_eq!(call(&s, Method::GET, "/v1/auth/me", Some(VIEWER)).await.0, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn k5_ui_static_strict_paths_and_headers() {
    let s = setup();
    let assets = Arc::new(UiAssets::from_files([
        ("index.html".to_string(), b"<!doctype html><div id=root></div>".to_vec()),
        ("assets/app-1.js".to_string(), b"console.log(1)".to_vec()),
    ]));
    let app = || router_with_ui(s.clone(), Some(assets.clone()));
    let (st, h, body) = call_on(app(), Method::GET, "/ui/", None, Value::Null).await;
    assert_eq!(st, StatusCode::OK);
    assert!(body.contains("root"));
    let csp = h["content-security-policy"].to_str().unwrap();
    assert!(csp.contains("frame-ancestors 'none'") && !csp.contains("unsafe-eval") && !csp.contains("http"));
    assert_eq!(h["x-content-type-options"], "nosniff");
    assert_eq!(h["referrer-policy"], "no-referrer");
    let (st, h, _) = call_on(app(), Method::GET, "/ui/assets/app-1.js", None, Value::Null).await;
    assert_eq!(st, StatusCode::OK);
    assert!(h["content-type"].to_str().unwrap().starts_with("text/javascript"));
    // SPA fallback for extension-less client routes only
    assert_eq!(call_on(app(), Method::GET, "/ui/pipelines/p1", None, Value::Null).await.0, StatusCode::OK);
    for bad in ["/ui/assets/missing.js", "/ui/../Cargo.toml", "/ui/%2e%2e/etc/passwd", "/ui/.env", "/ui/assets/..%2fx.js"] {
        assert_eq!(call_on(app(), Method::GET, bad, None, Value::Null).await.0, StatusCode::NOT_FOUND, "{bad}");
    }
    assert_eq!(call_on(app(), Method::GET, "/ui", None, Value::Null).await.0, StatusCode::PERMANENT_REDIRECT);
    // API is not swallowed by the fallback and still requires auth
    assert_eq!(call_on(app(), Method::GET, "/v1/pipelines", None, Value::Null).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(call_on(app(), Method::GET, "/v1/nope", None, Value::Null).await.0, StatusCode::UNAUTHORIZED);
    let (st, _, body) = call_on(app(), Method::GET, "/v1/nope", Some(ADMIN), Value::Null).await;
    assert_eq!(st, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(call_on(app(), Method::GET, "/", None, Value::Null).await.0, StatusCode::OK);
    // Without --ui-dir nothing under /ui is public
    assert_eq!(call(&s, Method::GET, "/ui/", None).await.0, StatusCode::UNAUTHORIZED);
}

#[test]
fn k5_ui_dir_loader_refuses_symlinks_and_missing_index() {
    let dir = std::env::temp_dir().join(format!("k5-ui-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("assets")).unwrap();
    assert!(UiAssets::load(&dir).is_err(), "index.html required");
    std::fs::write(dir.join("index.html"), "x").unwrap();
    std::fs::write(dir.join(".secret"), "x").unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink("/etc/passwd", dir.join("assets/passwd.txt")).unwrap();
    let assets = Arc::new(UiAssets::load(&dir).unwrap());
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let s = setup();
    rt.block_on(async {
        let app = || router_with_ui(s.clone(), Some(assets.clone()));
        assert_eq!(call_on(app(), Method::GET, "/ui/assets/passwd.txt", None, Value::Null).await.0, StatusCode::NOT_FOUND);
        assert_eq!(call_on(app(), Method::GET, "/ui/.secret", None, Value::Null).await.0, StatusCode::NOT_FOUND);
    });
    let _ = std::fs::remove_dir_all(&dir);
}
