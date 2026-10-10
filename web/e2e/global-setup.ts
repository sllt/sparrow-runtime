import { spawn } from "node:child_process";
import { chmodSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { api, BASE, PORT, SECRET_VALUE, SQL_SENTINEL, sha, TOKENS } from "./fixture";

const STREAM = { fields: [
  { name: "device_id", type: "utf8", nullable: false },
  { name: "temperature", type: "float64", nullable: true },
  { name: "humidity", type: "float64", nullable: true },
  { name: "ts", type: "timestamp_micros_utc", nullable: false },
  { name: "payload", type: "dynamic", nullable: true },
] };
const spec = (sql: string, client: string) => ({
  version: 1, stream: "sensors", sql,
  source: { kind: "mqtt", use_demo_io: true, topic: "sensors/json", client_id: client },
  sink: { kind: "http", use_demo_io: true },
  delivery: "live_best_effort", recovery: "restart_fresh",
});

export default async function () {
  const bin = process.env.SPARROW_SERVER_BIN;
  if (!bin) throw new Error("SPARROW_SERVER_BIN is required");
  const dir = mkdtempSync(join(tmpdir(), "k5-e2e-"));
  const authFile = join(dir, "auth.json");
  writeFileSync(authFile, JSON.stringify({ version: 1, principals: [
    { actor: "viewer-wang", role: "viewer", token_sha256: sha(TOKENS.viewer) },
    { actor: "operator-li", role: "operator", token_sha256: sha(TOKENS.operator) },
    { actor: "admin-ma", role: "admin", token_sha256: sha(TOKENS.admin) },
  ] }));
  chmodSync(authFile, 0o600);
  const env = { ...process.env };
  delete env.SPARROW_TOKEN;
  const child = spawn(resolve(bin), ["--bind", `127.0.0.1:${PORT}`, "--catalog", ":memory:", "--demo-io",
    "--auth-file", authFile, "--ui-dir", resolve("dist")], { env, stdio: ["ignore", "pipe", "pipe"] });
  let log = "";
  child.stdout.on("data", (d) => (log += d));
  child.stderr.on("data", (d) => (log += d));
  writeFileSync(join(dir, "pid"), String(child.pid));
  process.env.K5_E2E_PID = String(child.pid);
  process.env.K5_E2E_DIR = dir;
  for (let i = 0; ; i++) {
    try { if ((await fetch(`${BASE}/v1/health`)).ok) break; } catch { /* starting */ }
    if (i > 200) throw new Error(`server did not start:\n${log}`);
    await new Promise((r) => setTimeout(r, 100));
  }
  const a = TOKENS.admin;
  const must = async (p: Promise<{ status: number; text: string }>) => { const r = await p; if (r.status >= 300) throw new Error(`${r.status} ${r.text}`); return r; };
  await must(api("PUT", "/v1/secrets/demo_api_key", a, { value: SECRET_VALUE }));
  await must(api("PUT", "/v1/streams/sensors", a, STREAM));
  await must(api("PUT", "/v1/pipelines/sensor-alerts", a, spec(`SELECT device_id, temperature, ts FROM sensors WHERE temperature > 25 AND device_id <> '${SQL_SENTINEL}'`, "k5a")));
  await must(api("PUT", "/v1/pipelines/humidity-feed", a, spec("SELECT device_id, humidity, ts FROM sensors", "k5b")));
  await must(api("PUT", "/v1/pipelines/archive-draft", a, spec("SELECT device_id, ts FROM sensors", "k5c")));
  await must(api("POST", "/v1/pipelines/sensor-alerts/start", a, {}));
  await must(api("POST", "/v1/pipelines/humidity-feed/start", a, {}));
  await new Promise((r) => setTimeout(r, 1500));
  await must(api("POST", "/v1/pipelines/humidity-feed/stop", a, {}));
  // Background feeder keeps the live pipeline flowing during the smoke.
  const feeder = spawn(process.execPath, ["-e", `setInterval(()=>fetch(${JSON.stringify(BASE)}+"/v1/demo/publish-fixture",{method:"POST",headers:{authorization:"Bearer ${TOKENS.operator}"}}).catch(()=>{}),700)`], { stdio: "ignore", detached: false });
  writeFileSync(join(dir, "feeder"), String(feeder.pid));
  return async () => { feeder.kill(); child.kill("SIGTERM"); };
}
