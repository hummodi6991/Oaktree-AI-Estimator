# Expansion Advisor screenshot harness (dev-only)

These files render the **real** Expansion Advisor React components with
representative Riyadh sample data so we can capture authentic UI screenshots
for the user guide (`docs/Expansion-Advisor-User-Guide.pptx`). Nothing here
ships in the application build.

## Files

- `../shots.html` — standalone HTML entry (Vite serves/builds it).
- `main.tsx` — mounts one panel (`?panel=brief|results|compare|report`) with
  sample candidate / compare / report fixtures.
- `../vite.shots.config.ts` — builds `shots.html` to `_shots/dist/`.
- `shoot.mjs` — Playwright script that screenshots each panel into `_shots/out/`.

## Regenerate the screenshots

Playwright is intentionally **not** a project dependency (keeps the app's
dependency tree lean), so install it ad-hoc:

```bash
cd frontend
npm i -D playwright @playwright/test   # ad-hoc, do not commit
npx playwright install chromium

# 1. Build the static harness (no HMR → deterministic render)
npx vite build --config vite.shots.config.ts

# 2. Serve it
python3 -m http.server 5181 --bind 127.0.0.1 --directory _shots/dist &

# 3. Capture
node _shots/shoot.mjs            # writes _shots/out/*.png
```

Then copy the PNGs into `docs/assets/expansion-advisor/` and rebuild the deck:

```bash
python ../docs/generate_expansion_advisor_guide.py
```

> Notes for this sandbox: launch Chromium with `--no-sandbox`, and capture via
> CDP `Page.captureScreenshot` (Playwright's element screenshot waits on web
> fonts, which never resolve offline). Both are already handled in `shoot.mjs`.
