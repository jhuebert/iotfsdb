/**
 * Seeds a small amount of sample data before the e2e suite runs so the tests
 * have deterministic series to search and plot.
 */

import type { FullConfig } from "@playwright/test";

const E2E_PORT = process.env.E2E_PORT ?? "8080";
const BASE = `http://localhost:${E2E_PORT}`;

const SERIES_ID = "seed.temperature";
const MINUTES = 120;
const INTERVAL_MS = 60_000;

async function seed() {
  // Idempotent: remove any previous seed series first, then recreate it.
  const deleteResponse = await fetch(`${BASE}/v2/series/${SERIES_ID}`, { method: "DELETE" });
  if (deleteResponse.ok || deleteResponse.status === 404) {
    // expected: series removed or did not exist
  } else {
    throw new Error(`Failed to clear seed series: ${deleteResponse.status} ${await deleteResponse.text()}`);
  }

  const createResponse = await fetch(`${BASE}/v2/series`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      definition: {
        id: SERIES_ID,
        type: "FLOAT4",
        interval: INTERVAL_MS,
        partition: "DAY",
      },
      metadata: { room: "kitchen" },
    }),
  });
  if (!createResponse.ok && createResponse.status !== 400 && createResponse.status !== 409) {
    throw new Error(`Failed to seed series: ${createResponse.status} ${await createResponse.text()}`);
  }

  // Insert one value per minute for the last two hours.
  const now = Date.now();
  const values = Array.from({ length: MINUTES }, (_, i) => ({
    time: new Date(now - (MINUTES - i) * INTERVAL_MS).toISOString(),
    value: 20 + Math.sin(i / 5) * 3,
  }));
  const insertResponse = await fetch(`${BASE}/v2/data`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify([{ series: SERIES_ID, values }]),
  });
  if (!insertResponse.ok) {
    throw new Error(`Failed to seed data: ${insertResponse.status} ${await insertResponse.text()}`);
  }
}

export default async function globalSetup(_config: FullConfig) {
  // Retry a few times: the web server may still be accepting requests.
  for (let attempt = 0; attempt < 10; attempt++) {
    try {
      await seed();
      return;
    } catch (error) {
      console.warn(`Seeding attempt ${attempt + 1} failed: ${(error as Error).message}`);
      await new Promise((resolve) => setTimeout(resolve, 3000));
    }
  }
  throw new Error("Could not seed e2e data");
}
