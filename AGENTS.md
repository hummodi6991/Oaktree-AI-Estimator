# How Codex works on this repo

- One task per PR; run `pytest -q` before opening PR.
- Keep endpoints under `/v1/*`; update OpenAPI when behavior changes.
- No secrets; rely on GitHub OIDC for deploys.

## Deploy
- Deploy is via `.github/workflows/deploy-sccc.yml`; pushes to `main` trigger it.
- No secrets committed; OIDC + STS are used end-to-end.
