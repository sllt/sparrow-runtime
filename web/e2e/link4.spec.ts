import { expect, test, type Page } from "@playwright/test";
import { mkdirSync } from "node:fs";
import { api, TOKENS } from "./fixture";

const SHOTS = process.env.K5_SCREENS ?? "/workspace/notes/k5/screens";
mkdirSync(SHOTS, { recursive: true });
const shot = (page: Page, name: string) => page.screenshot({ path: `${SHOTS}/${name}.png`, fullPage: true });

async function login(page: Page, token: string, path = "/ui/", theme: "light" | "dark" = "light") {
  await page.goto(`${path}${path.includes("?") ? "&" : "?"}theme=${theme}`);
  await page.getByLabel("访问令牌").fill(token);
  await page.getByRole("button", { name: "登录" }).click();
}
const SQL = "SELECT device_id, COUNT(*) AS n FROM meters GROUP BY device_id, TUMBLE(PROCESSING_TIME, INTERVAL '1' SECOND)";
const spec = {
  version: 1, stream: "meters", sql: SQL,
  source: { kind: "mqtt", use_demo_io: true, topic: "sensors/json", client_id: "k54" },
  sink: { kind: "http", use_demo_io: true }, delivery: "live_best_effort", recovery: "restart_fresh",
};

for (const theme of ["light", "dark"] as const) {
  test(`link4: PT window preview on a virtual clock (${theme})`, async ({ page }) => {
    const id = `pt-preview-${theme}`;
    const seen: string[] = [];
    page.on("request", (r) => { if (r.url().includes("/v1/")) seen.push(r.url() + " " + (r.postData() ?? "")); });
    await api("PUT", "/v1/streams/meters", TOKENS.admin, { fields: [{ name: "device_id", type: "utf8", nullable: false }, { name: "temperature", type: "float64", nullable: true }, { name: "ts", type: "timestamp_micros_utc", nullable: false }] });
    const put = await api("PUT", `/v1/drafts/${id}`, TOKENS.operator, { pipeline: `pt-${theme}`, mode: "sql", text: JSON.stringify(spec, null, 2), metadata: {} });
    expect(put.status).toBe(201);
    await login(page, TOKENS.operator, `/ui/drafts/${id}`, theme);
    await expect(page.getByRole("heading", { name: id })).toBeVisible();
    const card = page.locator(".card", { has: page.getByRole("heading", { name: "受控预览" }) });
    await expect(card).toBeVisible();
    // Two rows, then advance the clock one second: the window fires only then.
    await card.getByRole("button", { name: "时钟 +1s" }).click();
    await card.getByRole("button", { name: "数据" }).click();
    await card.getByRole("button", { name: "时钟 +1s" }).click();
    await expect(card.getByLabel("第 3 步时钟")).toHaveValue("1000000");
    await expect(card.getByLabel("第 5 步时钟")).toHaveValue("2000000");
    await card.getByRole("button", { name: "运行" }).click();
    await expect(card.getByText(/输入 3 行 → 输出 3 行/)).toBeVisible();
    const steps = card.locator(".pv-steps tbody tr");
    await expect(steps).toHaveCount(5);
    await expect(steps.nth(0)).toContainText("—");
    await expect(steps.nth(2)).toContainText("2 行");
    await expect(steps.nth(4)).toContainText("1 行");
    await expect(card.getByText("未参与的配置：")).toBeVisible();
    await steps.nth(2).click();
    await expect(card.locator("table").nth(1).locator("tbody tr")).toHaveCount(2);
    await page.evaluate(() => window.scrollTo(0, 0));
    await shot(page, `44-preview-pt-${theme}`);
    // Backwards clock is caught locally and the run button is disabled.
    await card.getByLabel("第 5 步时钟").fill("10");
    await expect(card.getByText(/时钟不能倒退/)).toBeVisible();
    await expect(card.getByRole("button", { name: "运行" })).toBeDisabled();
    // Results are not part of the draft.
    expect((await api("GET", `/v1/drafts/${id}`, TOKENS.admin)).text).not.toContain("advance_clock");
    expect(seen.some((s) => s.includes("/v1/preview"))).toBe(true);
  });
}

test("link4: unsupported preview shapes are refused, not faked", async ({ page }) => {
  const bad = { ...spec, sql: "SELECT * FROM sensors", graph: undefined };
  const r = await api("POST", "/v1/preview", TOKENS.operator, { spec: { ...bad, sql: "SELECT device_id FROM sensors" }, events: [{ type: "eof" }, { type: "eof" }] });
  expect(r.status).toBe(400);
  const v = await api("POST", "/v1/preview", TOKENS.viewer, { sql: "SELECT * FROM sensors", events: [{ type: "eof" }] });
  expect(v.status).toBe(403);
  await login(page, TOKENS.operator, "/ui/drafts/pt-preview-light");
  const card = page.locator(".card", { has: page.getByRole("heading", { name: "受控预览" }) });
  await card.getByRole("button", { name: "重置" }).click();
  await card.getByLabel("第 1 步数据").fill('{"device_id": "d1", "temperature": "hot", "ts": 1}');
  await card.getByRole("button", { name: "运行" }).click();
  await expect(card.getByRole("alert")).toBeVisible();
  // A stream with a Dynamic column cannot be stepped: refused with the reason.
  const dyn = await api("POST", "/v1/preview", TOKENS.operator, { sql: "SELECT device_id, COUNT(*) AS n FROM sensors GROUP BY device_id, TUMBLE(PROCESSING_TIME, INTERVAL '1' SECOND)", events: [{ type: "eof" }] });
  expect(dyn.status).toBe(422);
  expect(dyn.text).toContain("preview cannot step this plan");
  await shot(page, "45-preview-error-light");
});
