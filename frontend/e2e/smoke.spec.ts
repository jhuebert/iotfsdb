import { expect, test } from "@playwright/test";

test.describe("SPA smoke", () => {
  test("serves the app shell and redirects legacy /ui links", async ({ page }) => {
    await page.goto("/ui/series");
    await expect(page).toHaveURL(/\/series$/);
    await expect(page.getByRole("heading", { name: "Series" })).toBeVisible();
    await expect(page.getByText("iotfsdb").first()).toBeVisible();
  });

  test("loads runtime config and shows the version", async ({ page }) => {
    await page.goto("/series");
    await expect(page.getByText(/v\d+\.\d+\.\d+/).first()).toBeVisible();
  });
});

test.describe("Series page", () => {
  test("lists and filters series", async ({ page }) => {
    await page.goto("/series");
    const search = page.getByLabel("Series filter");
    await expect(search).toBeVisible();
    await search.fill("seed.*");
    await expect(page.getByText("seed.temperature")).toBeVisible();
  });

  test("deep link with q restores the filter", async ({ page }) => {
    await page.goto("/series?q=seed.*");
    await expect(page.getByLabel("Series filter")).toHaveValue("seed.*");
    await expect(page.getByText("seed.temperature")).toBeVisible();
  });

  test("shows parse errors inline", async ({ page }) => {
    await page.goto("/series");
    const search = page.getByLabel("Series filter");
    await search.fill('room:"unbalanced');
    await expect(page.getByRole("alert")).toContainText("Unbalanced quote");
  });
});

test.describe("Data page", () => {
  test("queries and renders a chart for a selected series", async ({ page }) => {
    await page.goto("/data");
    // Auto-selects the first matches; the seeded series should appear as a checkbox.
    await expect(page.getByText("seed.temperature")).toBeVisible();
    await page.getByLabel("Select seed.temperature").check();
    await expect(page.locator("canvas")).toBeVisible({ timeout: 15_000 });
    await expect(page.getByText("Values").first()).toBeVisible();
  });

  test("deep link restores query state", async ({ page }) => {
    await page.goto("/data?q=seed.*&preset=LAST_24_HOURS&sel=seed.temperature");
    await expect(page.getByLabel("Series filter")).toHaveValue("seed.*");
    await expect(page.locator("canvas")).toBeVisible({ timeout: 15_000 });
  });
});

test.describe("Transfer page", () => {
  test("export button is available", async ({ page }) => {
    await page.goto("/transfer");
    await expect(page.getByRole("button", { name: "Export Database" })).toBeVisible();
  });
});
