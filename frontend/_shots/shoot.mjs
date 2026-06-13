import { chromium } from "playwright";
import { mkdirSync, writeFileSync } from "node:fs";

const BASE = "http://127.0.0.1:5181/shots.html";
const OUT = "_shots/out";
mkdirSync(OUT, { recursive: true });

const shots = [
  { panel: "brief", file: "brief.png", clip: ".ea-panel" },
  { panel: "results", file: "results.png", clip: ".ea-panel" },
  { panel: "compare", file: "compare.png", clip: ".ea-drawer" },
  { panel: "report", file: "report.png", clip: ".ea-exec-modal" },
];

const browser = await chromium.launch({ args: ["--no-sandbox", "--disable-setuid-sandbox"] });
const ctx = await browser.newContext({
  viewport: { width: 1440, height: 1600 },
  deviceScaleFactor: 2,
});
const page = await ctx.newPage();
const errors = [];
page.on("console", (m) => { if (m.type() === "error") errors.push(m.text()); });
page.on("pageerror", (e) => errors.push("PAGEERROR: " + e.message));

// CDP screenshot bypasses Playwright's "wait for fonts" stabilization,
// which never resolves in this offline sandbox.
const cdp = await ctx.newCDPSession(page);

for (const s of shots) {
  errors.length = 0;
  await page.goto(`${BASE}?panel=${s.panel}`, { waitUntil: "load", timeout: 20000 });
  try {
    await page.waitForSelector(s.clip, { timeout: 15000, state: "attached" });
  } catch {
    console.log(`!! ${s.panel}: selector ${s.clip} not found`);
  }
  await page.waitForTimeout(800);
  const box = await page.evaluate((sel) => {
    const el = document.querySelector(sel);
    if (!el) return null;
    const r = el.getBoundingClientRect();
    // Use the true content height so a viewport-stretched container
    // (#root at 100vh) doesn't leave a tall empty band below the content.
    const height = Math.min(r.height, el.scrollHeight) || r.height;
    return { x: r.x, y: r.y, width: r.width, height };
  }, s.clip);
  if (!box || box.width < 5 || box.height < 5) {
    console.log(`!! ${s.panel}: empty box`, box, errors.slice(0, 2));
    continue;
  }
  const { data } = await cdp.send("Page.captureScreenshot", {
    format: "png",
    captureBeyondViewport: true,
    clip: { x: Math.max(0, box.x), y: Math.max(0, box.y), width: box.width, height: box.height, scale: 2 },
  });
  writeFileSync(`${OUT}/${s.file}`, Buffer.from(data, "base64"));
  console.log(`captured ${s.file}  (${Math.round(box.width)}x${Math.round(box.height)})${errors.length ? "  ERRORS: " + errors.slice(0, 2).join(" | ") : ""}`);
}

await browser.close();
console.log("done");
