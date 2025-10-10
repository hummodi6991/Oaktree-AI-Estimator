### 1) Build the React/Vite front-end
FROM node:20-alpine AS webbuild
WORKDIR /web
COPY frontend/ /web/frontend/
WORKDIR /web/frontend
# Use lockfile if present; otherwise fall back to install
RUN (npm ci || npm install) && npm run build

### 2) Build the FastAPI image and copy the static site
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y build-essential libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
COPY --from=webbuild /web/frontend/dist /app/frontend/dist

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
