import { defineConfig } from "@playwright/test";
import path from "node:path";

export default defineConfig({
  testDir: "./e2e",
  fullyParallel: false,
  retries: 0,
  workers: 1,
  reporter: "line",
  outputDir: process.env.ROCKETMQ_SRE_UI_SECURITY_ARTIFACTS
    ? path.resolve(process.env.ROCKETMQ_SRE_UI_SECURITY_ARTIFACTS)
    : "test-results",
  use: {
    baseURL: "http://127.0.0.1:3004",
    browserName: "chromium",
    headless: true,
    viewport: { width: 1600, height: 1000 },
  },
  webServer: {
    command: "npm run dev -- --host 127.0.0.1",
    env: {
      ...process.env,
      VITE_SRE_API_MODE: "mock",
      VITE_SRE_AUTH_MODE: "development",
    },
    reuseExistingServer: false,
    timeout: 120_000,
    url: "http://127.0.0.1:3004/?demo=1",
  },
});
