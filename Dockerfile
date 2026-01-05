### 1) Build the React/Vite front-end
FROM public.ecr.aws/docker/library/node:20 AS frontend-build
WORKDIR /app/frontend
COPY frontend/package*.json ./
RUN npm ci
COPY frontend/ .
RUN npm run build

### 2) Build the FastAPI image and copy the static site
FROM public.ecr.aws/docker/library/python:3.11-slim
WORKDIR /app

RUN apt-get update && apt-get install -y build-essential libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy only what the backend needs (avoid copying the huge frontend sources/tiles again)
COPY app/ ./app/
COPY alembic/ ./alembic/
COPY alembic.ini .
COPY models/ ./models/
COPY README.md .

# Bring in the compiled UI and supporting public assets
COPY --from=frontend-build /app/frontend/dist /app/frontend/dist
COPY --from=frontend-build /app/frontend/public /app/frontend/public

# Serve tiles from the packaged static tiles and stay offline
ENV TILE_CACHE_DIR=/app/frontend/dist/static-tiles
ENV TILE_OFFLINE_ONLY=true

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
