# How Codex works on this repo

- One task per PR; run `pytest` before opening PR.
- Keep endpoints under `/v1/*`; update OpenAPI when behavior changes.
- No secrets; rely on GitHub OIDC for GCP deploys.
