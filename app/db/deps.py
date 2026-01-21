from typing import Generator

from sqlalchemy.orm import Session

from app.db import session as db_session


def get_db() -> Generator[Session, None, None]:
    db = db_session.SessionLocal()
    try:
        yield db
    finally:
        db.close()
