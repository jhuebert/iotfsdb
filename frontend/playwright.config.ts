import { defineConfig, devices } from "@playwright/test";

/**
 * E2E tests run against the real Spring Boot app (in-memory storage) started
 * by Gradle. The SPA is served from the jar/static resources, so the backend
 * must be built (`./gradlew build`) before running `npm run e2e`.
 */

const E2E_PORT = process.env.E2E_PORT ?? "8080";
const BASE_URL = `http://localhost:${E2E_PORT}`;

export default defineConfig({
  testDir: "./e2e",
  globalSetup: "./e2e/global-setup.ts",
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 2 : undefined,
  reporter: "list",
  use: {
    baseURL: BASE_URL,
    trace: "on-first-retry",
  },
  projects: [
    { name: "chromium", use: { ...devices["Desktop Chrome"] } },
    { name: "firefox", use: { ...devices["Desktop Firefox"] } },
  ],
  webServer: {
    command: `../gradlew -p .. bootRun --args="--server.port=${E2E_PORT} --iotfsdb.persistence.root=memory --spring.main.banner-mode=off"`,
    url: `${BASE_URL}/v2/ui/config`,
    reuseExistingServer: !process.env.CI,
    timeout: 300_000,
  },
});
