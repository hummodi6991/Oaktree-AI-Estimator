import os

from dotenv import load_dotenv

load_dotenv()


class Settings:
    APP_ENV: str = os.getenv("APP_ENV", "local")
    APP_NAME: str = os.getenv("APP_NAME", "oaktree-estimator")
    DB_USER: str = os.getenv("POSTGRES_USER", "oaktree")
    DB_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "devpass")
    DB_NAME: str = os.getenv("POSTGRES_DB", "oaktree")
    DB_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
    DB_PORT: int = int(os.getenv("POSTGRES_PORT", "5432"))


settings = Settings()
