//! Production HTTP management client: no Kernel, demo broker or testkit.
use reqwest::{Method, Url};
use serde_json::{json, Value};
use std::io::Read;
use std::time::Duration;

type Result<T> = std::result::Result<T, String>;
fn read_plugin(manifest:&str,artifact:&str)->Result<Value>{
    use base64::Engine;
    let manifest=read_input(manifest)?;
    let file=std::fs::File::open(artifact).map_err(|e|format!("plugin artifact: {e}"))?;
    if !file.metadata().map_err(|e|format!("plugin artifact: {e}"))?.is_file(){return Err("plugin artifact must be a regular file".into());}
    let mut bytes=Vec::new();file.take(4*1024*1024+1).read_to_end(&mut bytes).map_err(|e|format!("plugin artifact: {e}"))?;
    if bytes.len()>4*1024*1024{return Err("plugin artifact exceeds 4MiB".into());}
    Ok(json!({"manifest":manifest,"artifact_base64":base64::engine::general_purpose::STANDARD.encode(bytes)}))
}
fn digest(s:&str)->Result<String>{if s.len()==64&&s.bytes().all(|c|c.is_ascii_digit()||(b'a'..=b'f').contains(&c)){Ok(s.into())}else{Err("plugin manifest hash must be 64 lowercase hex digits".into())}}
const INPUT_CAP: u64 = 64 * 1024;
const RESPONSE_CAP: usize = 2 * 1024 * 1024;
struct Command {
    base: Url,
    timeout: Duration,
    method: Method,
    segments: Vec<String>,
    body: Option<Value>,
    etag: Option<String>,
    output: Option<String>,
}
fn read_input(path: &str) -> Result<Value> {
    let mut bytes = Vec::new();
    if path == "-" {
        std::io::stdin().take(INPUT_CAP + 1).read_to_end(&mut bytes)
    } else {
        std::fs::File::open(path)
            .map_err(|e| format!("input: {e}"))?
            .take(INPUT_CAP + 1)
            .read_to_end(&mut bytes)
    }
    .map_err(|e| format!("input: {e}"))?;
    if bytes.len() as u64 > INPUT_CAP {
        return Err("input exceeds 64 KiB".into());
    }
    serde_json::from_slice(&bytes).map_err(|_| "input must be JSON".into())
}
fn name(s: &str) -> Result<String> {
    if s.is_empty()
        || s.len() > 64
        || !s
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "_.-".contains(c))
    {
        Err("name must be 1..=64 characters in [A-Za-z0-9_.-]".into())
    } else {
        Ok(s.into())
    }
}
fn parse(args: &[String]) -> Result<Command> {
    let mut positional = Vec::new();
    let mut base = std::env::var("SPARROW_URL").unwrap_or_else(|_| "http://127.0.0.1:43180".into());
    let mut ms = 15_000;
    let mut etag = None;
    let mut output = None;
    let mut revision = None;
    let mut snapshot = None;
    let mut allow_http = false;
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--allow-insecure-http" => allow_http = true,
            "--url" | "--timeout-ms" | "--if-match" | "--output" | "--revision"
            | "--snapshot-id" => {
                let value = it.next().ok_or_else(|| format!("{arg} needs a value"))?;
                match arg.as_str() {
                    "--url" => base = value.clone(),
                    "--timeout-ms" => ms = value.parse::<u64>().map_err(|_| "invalid timeout")?,
                    "--if-match" => etag = Some(value.clone()),
                    "--output" => output = Some(value.clone()),
                    "--revision" => {
                        revision = Some(value.parse::<u64>().map_err(|_| "invalid revision")?)
                    }
                    _ => snapshot = Some(value.parse::<u64>().map_err(|_| "invalid snapshot id")?),
                }
            }
            flag if flag.starts_with("--") => return Err(format!("unknown option {flag}")),
            _ => positional.push(arg.as_str()),
        }
    }
    if !(100..=125_000).contains(&ms) {
        return Err("timeout must be 100..=125000 ms".into());
    }
    let base = Url::parse(&base).map_err(|_| "invalid management URL")?;
    if !matches!(base.scheme(), "http" | "https")
        || base.host_str().is_none()
        || !base.username().is_empty()
        || base.password().is_some()
        || base.query().is_some()
        || base.fragment().is_some()
        || base.path() != "/"
    {
        return Err(
            "URL must be an http(s) origin without credentials, path, query or fragment".into(),
        );
    }
    let loopback = base.host_str().is_some_and(|h| {
        h == "localhost"
            || h.trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    });
    if base.scheme() == "http" && !loopback && !allow_http {
        return Err(
            "remote plaintext management requires --allow-insecure-http; prefer HTTPS".into(),
        );
    }
    let (method, segments, body) = match positional.as_slice() {
        ["plugins"] => (Method::GET,vec!["plugins".into()],None),
        ["plugin-install",manifest,artifact] => (Method::POST,vec!["plugins".into(),"install".into()],Some(read_plugin(manifest,artifact)?)),
        [op @ ("plugin-enable"|"plugin-disable"|"plugin-uninstall"),id] => {
            let id=digest(id)?;let action=op.trim_start_matches("plugin-");
            let body=if action=="enable"{json!({"approve_manifest_sha256":id})}else{json!({})};
            (Method::POST,vec!["plugins".into(),id,action.into()],Some(body))
        },
        ["health"] => (Method::GET, vec!["health".into()], None),
        ["capabilities"] => (Method::GET, vec!["capabilities".into()], None),
        ["pipelines"] => (Method::GET, vec!["pipelines".into()], None),
        ["streams"] => (Method::GET, vec!["streams".into()], None),
        ["validate", file] | ["explain", file] | ["query", file] => (
            Method::POST,
            vec![positional[0].into()],
            Some(read_input(file)?),
        ),
        ["put-stream", id, file] => (
            Method::PUT,
            vec!["streams".into(), name(id)?],
            Some(read_input(file)?),
        ),
        ["put-pipeline", id, file] => (
            Method::PUT,
            vec!["pipelines".into(), name(id)?],
            Some(read_input(file)?),
        ),
        [op @ ("status" | "diagnose" | "checkpoints"), id] => (
            Method::GET,
            vec!["pipelines".into(), name(id)?, (*op).into()],
            None,
        ),
        [op @ ("start" | "stop" | "kill" | "checkpoint" | "restore"), id] => {
            let body = match *op {
                "start" => json!({"revision":revision}),
                "restore" => json!({"snapshot_id":snapshot}),
                _ => json!({}),
            };
            (
                Method::POST,
                vec!["pipelines".into(), name(id)?, (*op).into()],
                Some(body),
            )
        }
        _ => return Err("unknown command or wrong argument count; use --help".into()),
    };
    for (set, expected, label) in [
        (revision.is_some(), "start", "--revision"),
        (snapshot.is_some(), "restore", "--snapshot-id"),
        (etag.is_some(), "put-pipeline", "--if-match"),
        (output.is_some(), "diagnose", "--output"),
    ] {
        if set && positional.first() != Some(&expected) {
            return Err(format!("{label} is {expected}-only"));
        }
    }
    Ok(Command {
        base,
        timeout: Duration::from_millis(ms),
        method,
        segments,
        body,
        etag,
        output,
    })
}
async fn execute(command: Command, token: &str) -> Result<(Value, bool)> {
    let mut url = command.base;
    {
        let mut path = url.path_segments_mut().map_err(|_| "invalid URL")?;
        path.clear().push("v1");
        for s in command.segments {
            path.push(&s);
        }
    }
    let client = reqwest::Client::builder()
        .timeout(command.timeout)
        .connect_timeout(command.timeout.min(Duration::from_secs(5)))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|e| format!("client: {e}"))?;
    let mut request = client.request(command.method, url).bearer_auth(token);
    if let Some(body) = command.body {
        request = request.json(&body);
    }
    if let Some(etag) = command.etag {
        request = request.header("If-Match", etag);
    }
    let mut response = request
        .send()
        .await
        .map_err(|e| format!("management request failed: {e}"))?;
    let success = response.status().is_success();
    let code = response.status().as_u16();
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| format!("response: {e}"))?
    {
        if bytes.len().saturating_add(chunk.len()) > RESPONSE_CAP {
            return Err("response exceeds 2 MiB".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    let value: Value = match serde_json::from_slice(&bytes) {
        Ok(value) => value,
        Err(_) if !success => {
            json!({"error":{"code":"http_error","http_status":code,"message":"management endpoint returned a non-JSON error response"}})
        }
        Err(_) => return Err(format!("HTTP {code}: response is not JSON")),
    };
    if let Some(path) = command.output {
        if !success {
            return Ok((value, false));
        }
        let mut options = std::fs::OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let writing = (|| {
            let file = options
                .open(path)
                .map_err(|e| format!("create diagnostic output: {e}"))?;
            serde_json::to_writer_pretty(file, &value)
                .map_err(|e| format!("write diagnostic output: {e}"))
        })();
        if let Err(error) = writing {
            println!("{value}");
            return Err(format!(
                "{error}; diagnostic returned on stdout; output file may be incomplete"
            ));
        }
    }
    Ok((value, success))
}
fn help() {
    println!(
        "sparrowctl — authenticated JSON management client\n\
commands: health | capabilities | streams | pipelines\n\
  plugins | plugin-install MANIFEST_JSON ARTIFACT\n\
  plugin-enable MANIFEST_SHA256 | plugin-disable MANIFEST_SHA256 | plugin-uninstall MANIFEST_SHA256\n\
  validate FILE | explain FILE | query FILE | put-stream NAME FILE\n\
  put-pipeline NAME FILE [--if-match ETAG]\n\
  status NAME | diagnose NAME [--output NEW_FILE] | checkpoints NAME\n\
  start NAME [--revision N] | stop NAME | kill NAME\n\
  checkpoint NAME | restore NAME [--snapshot-id N]\n\
options: --url ORIGIN (or SPARROW_URL), --timeout-ms 100..125000, --allow-insecure-http\n\
authentication: SPARROW_TOKEN (no token command-line option). FILE=- reads bounded JSON stdin.\n\
exit: 0 success, 1 local/transport failure, 2 HTTP API rejection. No exactly-once promise."
    );
}
#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() || args.iter().any(|a| a == "--help" || a == "-h") {
        help();
        return;
    }
    if args.as_slice() == ["--version"] {
        println!(
            "{}",
            json!({"version":env!("CARGO_PKG_VERSION"),"commit":option_env!("SPARROW_BUILD_COMMIT").unwrap_or("unknown")})
        );
        return;
    }
    let result = async {
        let command = parse(&args)?;
        let token = std::env::var("SPARROW_TOKEN").map_err(|_| "SPARROW_TOKEN is required")?;
        if token.is_empty() {
            return Err("SPARROW_TOKEN is empty".into());
        }
        execute(command, &token).await
    }
    .await;
    match result {
        Ok((value, ok)) => {
            println!("{value}");
            if !ok {
                std::process::exit(2);
            }
        }
        Err(error) => {
            eprintln!(
                "{}",
                json!({"error":{"code":"client_error","message":error}})
            );
            std::process::exit(1);
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|s| (*s).into()).collect()
    }
    fn fixture(
        status: &str,
        body: Vec<u8>,
        delay: Duration,
    ) -> (String, std::thread::JoinHandle<()>) {
        use std::io::Write;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let status = status.to_string();
        let task = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut request = [0; 4096];
            let n = socket.read(&mut request).unwrap();
            assert!(String::from_utf8_lossy(&request[..n]).contains("Bearer test-token"));
            std::thread::sleep(delay);
            let header=format!("HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nLocation: http://127.0.0.1:1/secret\r\nConnection: close\r\n\r\n",body.len());
            let _ = socket.write_all(header.as_bytes());
            let _ = socket.write_all(&body);
        });
        (url, task)
    }
    #[tokio::test]
    async fn production_cli_http_errors_redirects_timeout_and_body_bounds() {
        for (status, body, delay, expected) in [
            ("200 OK", b"{\"ok\":true}".to_vec(), 0, 0),
            ("401 Unauthorized", b"{\"error\":\"denied\"}".to_vec(), 0, 2),
            ("302 Found", b"{}".to_vec(), 0, 2),
            (
                "502 Bad Gateway",
                b"<html>upstream unavailable</html>".to_vec(),
                0,
                2,
            ),
            ("200 OK", b"{}".to_vec(), 300, 1),
            ("200 OK", vec![b' '; RESPONSE_CAP + 1], 0, 1),
        ] {
            let (url, task) = fixture(status, body, Duration::from_millis(delay));
            let command = parse(&args(&[
                "health",
                "--url",
                &url,
                "--timeout-ms",
                if delay > 0 { "100" } else { "2000" },
            ]))
            .unwrap();
            let result = execute(command, "test-token").await;
            let code = match result {
                Ok((_, true)) => 0,
                Ok((_, false)) => 2,
                Err(_) => 1,
            };
            assert_eq!(code, expected, "{status} delay={delay}");
            task.join().unwrap();
        }
    }
    #[test]
    fn production_cli_rejects_unsafe_urls_names_and_misplaced_options() {
        for a in [
            vec!["status", "../secret"],
            vec!["health", "--url", "http://user:pass@localhost"],
            vec!["health", "--url", "http://192.0.2.1"],
            vec!["health", "--revision", "1"],
            vec!["restore", "p", "--snapshot-id", "no"],
        ] {
            assert!(parse(&args(&a)).is_err(), "{a:?}");
        }
        assert!(parse(&args(&["status", "p", "--url", "http://127.0.0.1:43180"])).is_ok());
        assert!(parse(&args(&["restore", "p", "--snapshot-id", "3"])).is_ok());
    }
    #[test]
    fn plugins_cli_explicit_digest_and_management_routes(){
        let hash="a".repeat(64);
        for action in ["plugin-enable","plugin-disable","plugin-uninstall"]{
            let command=parse(&args(&[action,&hash])).unwrap();assert_eq!(command.segments,vec!["plugins",hash.as_str(),action.trim_start_matches("plugin-")]);
            if action=="plugin-enable"{assert_eq!(command.body.unwrap()["approve_manifest_sha256"],hash);}
            for bad in ["../etc", "latest", "ABC", ""]{assert!(parse(&args(&[action,bad])).is_err());}
        }
        assert_eq!(parse(&args(&["plugins"])).unwrap().method,Method::GET);
    }
}
