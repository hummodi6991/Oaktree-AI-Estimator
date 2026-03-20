# How Codex works on this repo

- One task per PR; run `pytest -q` before opening PR.
- Keep endpoints under `/v1/*`; update OpenAPI when behavior changes.
- No secrets committed to the repo.
- Prefer documenting and coding against the **currently implemented** auth/deploy flow, not an aspirational future state.

## Deploy
- Deploy is via `.github/workflows/deploy-sccc.yml`; pushes to `main` trigger it.
- Current ACK deploys are triggered from GitHub Actions on push to `main`.
- Current Alibaba deploy auth is **not** OIDC + STS end-to-end; the workflow currently uses Alibaba Access Key / Secret credentials.
- App auth currently supports `disabled` and `api_key`; `oidc` exists in config but is still a placeholder until fully wired.
- Do not claim OIDC is fully live unless the workflow and app auth code have actually been updated to support it end-to-end.
