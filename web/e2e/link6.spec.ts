import { expect, test, type Page } from "@playwright/test";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { api, BASE, TOKENS } from "./fixture";

const SHOTS = process.env.K5_SCREENS ?? "/workspace/notes/k5/screens";
mkdirSync(SHOTS, { recursive: true });
const shot = async (page: Page, name: string) => { await page.evaluate(() => window.scrollTo(0, 0)); await page.screenshot({ path: `${SHOTS}/${name}.png`, fullPage: true }); };
async function login(page: Page, token: string, path: string, theme: "light" | "dark" = "light") {
  await page.goto(`${path}?theme=${theme}`);
  await page.getByLabel("访问令牌").fill(token);
  await page.getByRole("button", { name: "登录" }).click();
}
/** Offline guarantee: every request must stay on the local server. */
function offline(page: Page): string[] {
  const remote: string[] = [];
  page.on("request", (r) => { if (!r.url().startsWith(BASE) && !r.url().startsWith("blob:") && !r.url().startsWith("data:")) remote.push(r.url()); });
  return remote;
}

for (const theme of ["light", "dark"] as const) {
  test(`link6: export redacts, import is reviewed and lands as unpublishable drafts (${theme})`, async ({ page }) => {
    const remote = offline(page);
    const name = `bx-${theme}`;
    const r = await api("PUT", `/v1/pipelines/${name}`, TOKENS.operator, {
      version: 1, stream: "sensors", sql: "SELECT device_id, temperature, ts FROM sensors WHERE temperature > 77",
      source: { kind: "mqtt", use_demo_io: true, topic: "sensors/json", client_id: `bx-${theme}` },
      sink: { kind: "http", use_demo_io: true, url: "https://hooks.plant.internal/x?token=zzz" }, delivery: "live_best_effort", recovery: "restart_fresh",
    });
    expect(r.status, r.text).toBeLessThan(300);
    await login(page, TOKENS.operator, "/ui/bundles", theme);
    await expect(page.getByRole("heading", { name: "配置导入导出" })).toBeVisible();
    await page.getByRole("group", { name: "选择流水线" }).getByLabel(name).check();
    const dl = page.waitForEvent("download");
    await page.getByRole("button", { name: /导出 1 条/ }).click();
    const file = await (await dl).path();
    const text = readFileSync(file!, "utf8");
    expect(text).not.toContain("hooks.plant.internal");
    expect(text).not.toContain("> 77");
    expect(text).toContain("__SPARROW_FILL_IN__");
    await expect(page.getByText(/已导出 1 条流水线；\d+ 处字段被替换为待填写标记/)).toBeVisible();
    await page.getByLabel("包含 SQL / Graph 逻辑").check();
    await expect(page.getByText(/SQL 字面量和图参数可能包含敏感值/)).toBeVisible();
    // Import the redacted bundle as drafts with a distinct prefix.
    const path = `/tmp/k56-${theme}.json`;
    writeFileSync(path, text);
    await page.getByLabel("配置包文件").setInputFiles(path);
    await page.getByLabel("草稿 ID 前缀").fill(`imp-${theme}-`);
    await page.getByRole("button", { name: "预览" }).click();
    const pv = page.getByLabel("导入预览");
    await expect(pv.getByText("更新已有流水线")).toBeVisible();
    await expect(pv.getByText("Stream sensors：已存在且一致")).toBeVisible();
    await expect(pv.locator("[title*=\"sink.url\"]")).toBeVisible();
    await shot(page, `60-bundle-preview-${theme}`);
    await page.getByRole("button", { name: "确认导入为草稿" }).click();
    await expect(page.getByText(/已创建 1 个草稿；未发布，未启动/)).toBeVisible();
    await shot(page, `61-bundle-imported-${theme}`);
    // Re-importing the same bundle is blocked (draft exists) before any write.
    const again = await api("POST", "/v1/bundles/import/preview", TOKENS.operator, { bundle: JSON.parse(text), draft_prefix: `imp-${theme}-` });
    expect(again.text).toContain("already exists");
    // The imported draft opens in the editor and cannot be published.
    await page.getByRole("link", { name: `打开 imp-${theme}-${name}` }).click();
    await expect(page).toHaveURL(new RegExp(`/ui/drafts/imp-${theme}-${name}`));
    const d = await api("GET", `/v1/drafts/imp-${theme}-${name}`, TOKENS.operator);
    expect(d.status).toBe(200);
    const etag = JSON.parse(d.text).etag;
    const p = await api("POST", `/v1/drafts/imp-${theme}-${name}/publish`, TOKENS.operator, { operation_id: `imp-pub-${theme}-0001`, draft_etag: etag, base_etag: "rev-1" });
    expect(p.status).toBe(400);
    expect(p.text).toContain("fill-in");
    expect(remote, remote.join("\n")).toEqual([]);
  });
}

test("link6: UI/server contract mismatch forces read-only; viewer has no bundle access", async ({ page }) => {
  const remote = offline(page);
  await page.route("**/v1/auth/me", async (route) => {
    const res = await route.fetch();
    const body = await res.json();
    await route.fulfill({ response: res, json: { ...body, ui_contract: 99 } });
  });
  await login(page, TOKENS.admin, "/ui/", "light");
  await expect(page.getByRole("alert").filter({ hasText: "界面合同版本 1 与服务端 99 不一致" })).toBeVisible();
  await expect(page.getByText("只读", { exact: true })).toBeVisible();
  await expect(page.getByRole("link", { name: /导入导出/ })).toHaveCount(0);
  await shot(page, "62-contract-mismatch-light");
  await page.unroute("**/v1/auth/me");
  const v = await api("GET", "/v1/bundles/export?pipelines=x", TOKENS.viewer);
  expect(v.status).toBe(403);
  const i = await api("POST", "/v1/bundles/import/preview", TOKENS.viewer, { bundle: {} });
  expect(i.status).toBe(403);
  expect(remote).toEqual([]);
});
