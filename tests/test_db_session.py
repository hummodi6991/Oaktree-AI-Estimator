import importlib


def test_database_url_prefers_env(monkeypatch):
    env_url = "postgresql+psycopg://x:y@remote:5432/db?sslmode=require"
    monkeypatch.setenv("DATABASE_URL", env_url)
    monkeypatch.delenv("PGSSLMODE", raising=False)
    monkeypatch.delenv("DB_SSLMODE", raising=False)

    import app.db.session as session

    session = importlib.reload(session)

    assert "remote" in session.DATABASE_URL
    assert "localhost" not in session.DATABASE_URL

    monkeypatch.delenv("DATABASE_URL", raising=False)
    importlib.reload(session)


def test_does_not_append_sslmode_when_present(monkeypatch):
    env_url = "postgresql+psycopg://x:y@remote:5432/db?sslmode=require"
    monkeypatch.setenv("DATABASE_URL", env_url)
    monkeypatch.setenv("PGSSLMODE", "require")
    monkeypatch.delenv("DB_SSLMODE", raising=False)

    import app.db.session as session

    session = importlib.reload(session)

    assert session.DATABASE_URL.count("sslmode=require") == 1
    assert "('require','require')" not in session.DATABASE_URL

    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("PGSSLMODE", raising=False)
    importlib.reload(session)
