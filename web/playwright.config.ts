import { defineConfig } from "@playwright/test";

// Chromium smoke for K5 link 1 against a real sparrow-server binary
// (SPARROW_SERVER_BIN) serving web/dist via --ui-dir. See e2e/fixture.ts.
export default defineConfig({
  testDir: "e2e",
  timeout: 90_000,
  workers: 1,
  reporter: [["list"]],
  globalSetup: "./e2e/global-setup.ts",
  use: {
    baseURL: `http://127.0.0.1:${process.env.K5_PORT ?? "43991"}`,
    browserName: "chromium",
    viewport: { width: 1440, height: 900 },
    locale: "zh-CN",
    timezoneId: "Asia/Shanghai",
  },
});
