import os

from sqlalchemy import create_engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.orm import sessionmaker

from app.core.config import settings

sslmode = os.getenv("PGSSLMODE") or os.getenv("DB_SSLMODE")
env_database_url = os.getenv("DATABASE_URL")
source = "env" if env_database_url else "fallback"
DATABASE_URL = env_database_url or (
    f"postgresql+psycopg://{settings.DB_USER}:{settings.DB_PASSWORD}"
    f"@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
)
if sslmode:
    sep = "&" if "?" in DATABASE_URL else "?"
    DATABASE_URL = f"{DATABASE_URL}{sep}sslmode={sslmode}"

if os.getenv("DB_DEBUG") == "1":
    try:
        selected_host = make_url(DATABASE_URL).host or ""
    except Exception:
        selected_host = "unknown"
    print(f"[DB_DEBUG] using {source} database host '{selected_host}'")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
