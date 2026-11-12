import { defineConfig } from "@playwright/test";
import os from "node:os";
import path from "node:path";

const baseURL = process.env.ROCKETMQ_SRE_UI_HTTP_BASE_URL ?? "http://127.0.0.1:3004";

export default defineConfig({
  testDir: "./e2e",
  testMatch: "evidence-isolation.spec.ts",
  fullyParallel: false,
  retries: 0,
  workers: 1,
  reporter: "line",
  outputDir: process.env.ROCKETMQ_SRE_UI_HTTP_ARTIFACTS
    ? path.resolve(process.env.ROCKETMQ_SRE_UI_HTTP_ARTIFACTS)
    : path.join(os.tmpdir(), "rocketmq-sre-http-e2e"),
  use: {
    baseURL,
    browserName: "chromium",
    headless: true,
    viewport: { width: 1440, height: 900 },
  },
  webServer: {
    command: "npm run dev -- --host 127.0.0.1",
    env: {
      ...process.env,
      VITE_SRE_API_MODE: "http",
      VITE_SRE_DEMO_MODE: "false",
      VITE_SRE_AUTH_MODE: "oidc",
      VITE_SRE_OIDC_AUTHORITY: new URL("/oidc", baseURL).href,
      VITE_SRE_OIDC_CLIENT_ID: "sre-http-regression",
    },
    url: baseURL,
    reuseExistingServer: false,
    timeout: 120_000,
  },
});
