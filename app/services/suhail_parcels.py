import logging

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def refresh_suhail_parcels_mat(db: Session) -> None:
    try:
        db.execute(text("REFRESH MATERIALIZED VIEW CONCURRENTLY public.suhail_parcels_mat"))
        db.commit()
    except SQLAlchemyError as exc:
        logger.info(
            "Concurrent refresh failed for public.suhail_parcels_mat, falling back: %s",
            exc,
        )
        db.rollback()
        db.execute(text("REFRESH MATERIALIZED VIEW public.suhail_parcels_mat"))
        db.commit()

    db.execute(text("ANALYZE public.suhail_parcels_mat"))
    db.commit()
