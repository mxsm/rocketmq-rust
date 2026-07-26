import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "node:path";

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  server: {
    host: "0.0.0.0",
    allowedHosts: ["terminal.local"],
    port: 3004,
    proxy: {
      "/v1": {
        target: process.env.ROCKETMQ_SRE_API_URL ?? "http://127.0.0.1:8090",
        changeOrigin: true,
      },
      "/healthz": {
        target: process.env.ROCKETMQ_SRE_API_URL ?? "http://127.0.0.1:8090",
        changeOrigin: true,
      },
      "/readyz": {
        target: process.env.ROCKETMQ_SRE_API_URL ?? "http://127.0.0.1:8090",
        changeOrigin: true,
      },
    },
  },
});
