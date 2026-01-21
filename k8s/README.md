# Kubernetes secrets

Create or update the app secret with API key authentication settings:

```bash
kubectl create secret generic oaktree-app-env \
  --from-literal=AUTH_MODE=api_key \
  --from-literal=API_KEYS_JSON='{"tester-1":"tester-key"}' \
  --from-literal=ADMIN_API_KEYS_JSON='{"ceo":"ceo-key"}' \
  --dry-run=client -o yaml | kubectl apply -f -
```
