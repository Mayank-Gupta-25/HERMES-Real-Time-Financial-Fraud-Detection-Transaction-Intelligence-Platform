import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 3000,
    host: "0.0.0.0",
    proxy: {
      "/api/v1/analytics": {
        target: "http://localhost:8002",
        changeOrigin: true,
      },
      "/ws": {
        target: "ws://localhost:8003",
        ws: true,
      },
    },
  },
  test: {
    environment: "jsdom",
    globals: true,
  },
});
