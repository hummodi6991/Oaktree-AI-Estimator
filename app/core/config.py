import os

from dotenv import load_dotenv

# IMPORTANT:
# In CI, loading .env from the repo can override Settings defaults and break tests.
# GitHub Actions sets CI=true, so we skip dotenv there.
_CI = os.getenv("CI", "").strip().lower() in {"1", "true", "yes"}
if not _CI:
    load_dotenv()


class Settings:
    APP_ENV: str = os.getenv("APP_ENV", "local")
    APP_NAME: str = os.getenv("APP_NAME", "oaktree-estimator")
    DB_USER: str = os.getenv("POSTGRES_USER", "oaktree")
    DB_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "devpass")
    DB_NAME: str = os.getenv("POSTGRES_DB", "oaktree")
    DB_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
    DB_PORT: int = int(os.getenv("POSTGRES_PORT", "5432"))

    # --- Parcel source tables (ArcGIS is the default) ---
    # These are used by:
    # - /v1/tiles/parcels (outlines)
    # - /v1/geo/identify (click selection)
    # ArcGIS proxy view exposes: id, geom(4326), area_m2, perimeter_m, landuse_* fields.
    PARCEL_TILE_TABLE: str = os.getenv(
        "PARCEL_TILE_TABLE", "public.riyadh_parcels_arcgis_proxy"
    )
    PARCEL_IDENTIFY_TABLE: str = os.getenv(
        "PARCEL_IDENTIFY_TABLE", "public.riyadh_parcels_arcgis_proxy"
    )
    PARCEL_IDENTIFY_GEOM_COLUMN: str = os.getenv("PARCEL_IDENTIFY_GEOM_COLUMN", "geom")

    # --- External data & APIs (env-driven) ---
    # ArcGIS (البوابة المكانية) parcels/zoning
    ARCGIS_BASE_URL: str | None = os.getenv("ARCGIS_BASE_URL")
    ARCGIS_PARCEL_LAYER: int | None = (
        int(os.getenv("ARCGIS_PARCEL_LAYER")) if os.getenv("ARCGIS_PARCEL_LAYER") else None
    )
    ARCGIS_TOKEN: str | None = os.getenv("ARCGIS_TOKEN")

    # SAMA rates (open-data JSON endpoint)
    SAMA_OPEN_JSON: str | None = os.getenv("SAMA_OPEN_JSON")

    # REGA / SREM indicators (one or more CSV URLs; comma-separated)
    REGA_CSV_URLS: list[str] = [
        u.strip() for u in os.getenv("REGA_CSV_URLS", "").split(",") if u.strip()
    ]

    # Suhail (licensed partner API)
    SUHAIL_API_URL: str | None = os.getenv("SUHAIL_API_URL")
    SUHAIL_API_KEY: str | None = os.getenv("SUHAIL_API_KEY")

    # Restaurant Location Finder — optional API keys for enrichment
    GOOGLE_PLACES_API_KEY: str | None = os.getenv("GOOGLE_PLACES_API_KEY")
    FOURSQUARE_API_KEY: str | None = os.getenv("FOURSQUARE_API_KEY")

    # Parcels identify service configuration
    PARCEL_TARGET_SRID: int = int(os.getenv("PARCEL_TARGET_SRID", "4326"))
    PARCEL_IDENTIFY_TOLERANCE_M: float = float(
        os.getenv("PARCEL_IDENTIFY_TOLERANCE_M", "25.0")
    )
    PARCEL_ENVELOPE_PAD_M: float = float(os.getenv("PARCEL_ENVELOPE_PAD_M", "5.0"))
    PARCEL_SIMPLIFY_TOLERANCE_M: float = float(
        os.getenv("PARCEL_SIMPLIFY_TOLERANCE_M", "1.0")
    )

    # --- Expansion Advisor normalized tables ---
    EXPANSION_ROADS_TABLE: str = os.getenv("EXPANSION_ROADS_TABLE", "expansion_road_context")
    EXPANSION_PARKING_TABLE: str = os.getenv("EXPANSION_PARKING_TABLE", "expansion_parking_asset")
    EXPANSION_DELIVERY_TABLE: str = os.getenv("EXPANSION_DELIVERY_TABLE", "expansion_delivery_market")
    EXPANSION_RENT_TABLE: str = os.getenv("EXPANSION_RENT_TABLE", "expansion_rent_comp")
    EXPANSION_COMPETITOR_TABLE: str = os.getenv(
        "EXPANSION_COMPETITOR_TABLE", "expansion_competitor_quality"
    )

    # --- Realized demand (rating_count Δ) signal ---
    # When enabled AND the ``expansion_delivery_rating_history`` table has
    # ≥2 snapshots for the candidate's catchment, the service layer blends a
    # realized-demand score (rating_count growth per category per radius over
    # the last N days) into the supply-based _delivery_score().  Default OFF
    # so behavior is unchanged until history has accumulated.
    EXPANSION_REALIZED_DEMAND_ENABLED: bool = (
        os.getenv("EXPANSION_REALIZED_DEMAND_ENABLED", "").strip().lower()
        in {"1", "true", "yes", "on"}
    )
    EXPANSION_REALIZED_DEMAND_WINDOW_DAYS: int = int(
        os.getenv("EXPANSION_REALIZED_DEMAND_WINDOW_DAYS", "30")
    )
    EXPANSION_REALIZED_DEMAND_RADIUS_M: int = int(
        os.getenv("EXPANSION_REALIZED_DEMAND_RADIUS_M", "1200")
    )
    # Weight given to realized-demand vs listing-count when both are available.
    # 0.5 = equal blend; 1.0 = realized-demand only; 0.0 = listing-count only.
    EXPANSION_REALIZED_DEMAND_BLEND: float = float(
        os.getenv("EXPANSION_REALIZED_DEMAND_BLEND", "0.5")
    )

    # --- Expansion Advisor structured decision memo (Phase 1) ---
    # Model/token/temperature controls for the new structured memo path in
    # ``app.services.llm_decision_memo``. When ``EXPANSION_MEMO_STRUCTURED_ENABLED``
    # is false the service falls back to the legacy generic memo path byte-for-byte.
    EXPANSION_MEMO_MODEL: str = os.getenv("EXPANSION_MEMO_MODEL", "gpt-4o-mini")
    EXPANSION_MEMO_MAX_TOKENS: int = int(
        os.getenv("EXPANSION_MEMO_MAX_TOKENS", "2400")
    )
    EXPANSION_MEMO_TEMPERATURE: float = float(
        os.getenv("EXPANSION_MEMO_TEMPERATURE", "0.3")
    )
    EXPANSION_MEMO_STRUCTURED_ENABLED: bool = (
        os.getenv("EXPANSION_MEMO_STRUCTURED_ENABLED", "true").strip().lower()
        in {"1", "true", "yes", "on"}
    )

    # --- Expansion Advisor LLM shortlist reranking (Phase 2) ---
    # Bounded LLM reranking on the top deterministic shortlist. Default OFF.
    # When enabled, after the deterministic scorer + sort + LLM fuzzy tiebreak +
    # district balancing produce a candidate list, the top
    # min(len(candidates), EXPANSION_LLM_RERANK_SHORTLIST_SIZE) are sent to an
    # LLM that may rerank them within ±EXPANSION_LLM_RERANK_MAX_MOVE positions
    # from their deterministic rank. Candidates outside the shortlist cap pass
    # through unchanged with rerank_reason="outside_rerank_cap". Candidates
    # inside the cap with no LLM-proposed move pass through with
    # rerank_applied=False and rerank_reason=None.
    EXPANSION_LLM_RERANK_ENABLED: bool = (
        os.getenv("EXPANSION_LLM_RERANK_ENABLED", "").strip().lower()
        in {"1", "true", "yes", "on"}
    )
    EXPANSION_LLM_RERANK_MODEL: str = os.getenv(
        "EXPANSION_LLM_RERANK_MODEL", "gpt-4o-mini"
    )
    EXPANSION_LLM_RERANK_MAX_TOKENS: int = int(
        os.getenv("EXPANSION_LLM_RERANK_MAX_TOKENS", "2400")
    )
    EXPANSION_LLM_RERANK_TEMPERATURE: float = float(
        os.getenv("EXPANSION_LLM_RERANK_TEMPERATURE", "0.2")
    )
    EXPANSION_LLM_RERANK_MAX_MOVE: int = int(
        os.getenv("EXPANSION_LLM_RERANK_MAX_MOVE", "5")
    )
    EXPANSION_LLM_RERANK_SHORTLIST_SIZE: int = int(
        os.getenv("EXPANSION_LLM_RERANK_SHORTLIST_SIZE", "30")
    )
    EXPANSION_LLM_RERANK_MIN_SHORTLIST: int = int(
        os.getenv("EXPANSION_LLM_RERANK_MIN_SHORTLIST", "3")
    )

    # --- Expansion Advisor "best price-to-value" chip ---
    # Derived 0–100 value_score from estimated_revenue_index (location strength)
    # and rent_burden_score (rent vs. comparable peers). When enabled, populates
    # value_score / value_band on every candidate, applies a soft up/downrank
    # pass strictly inside the LLM rerank ±max_move envelope, and surfaces the
    # badge in the candidate card and report panel. Default ON: the feature
    # implements a product directive, not an experimental ML capability.
    # Setting EXPANSION_VALUE_SCORE_ENABLED=false skips the score, the
    # ordering pass, and (via the null value_band) the frontend badges.
    EXPANSION_VALUE_SCORE_ENABLED: bool = (
        os.getenv("EXPANSION_VALUE_SCORE_ENABLED", "true").strip().lower()
        in {"1", "true", "yes", "on"}
    )

    # --- Expansion Advisor decision-memo pre-warm (Phase 3) ---
    # After POST /searches returns, schedule a background task that
    # generates structured decision memos for the top-N candidates so the
    # first tap on a candidate in the UI is instant rather than incurring
    # a 3–5s LLM cold-call. The pre-warm task NEVER blocks the search
    # response and silently catches per-candidate failures so one bad memo
    # cannot abort the batch. Set TOP_N=0 (or ENABLED=false) to disable.
    EXPANSION_MEMO_PREWARM_ENABLED: bool = (
        os.getenv("EXPANSION_MEMO_PREWARM_ENABLED", "true").strip().lower()
        in {"1", "true", "yes", "on"}
    )
    EXPANSION_MEMO_PREWARM_TOP_N: int = int(
        os.getenv("EXPANSION_MEMO_PREWARM_TOP_N", "15")
    )
    # Wall-clock cap across the whole pre-warm batch. The check runs AFTER
    # each iteration, so the first candidate is ALWAYS attempted regardless
    # of how small the budget is; abandoned candidates stay un-warmed and
    # the lazy POST /decision-memo path will generate them on demand.
    #
    # Semantics:
    #   * > 0 → enforced budget (default 120s for a top-10 batch).
    #   * <= 0 → treated as UNBOUNDED (no wall-clock gate). The budget is an
    #     LLM-stuck-call safety valve, not an on/off switch — use
    #     ``EXPANSION_MEMO_PREWARM_ENABLED=false`` or
    #     ``EXPANSION_MEMO_PREWARM_TOP_N=0`` to disable pre-warm.
    EXPANSION_MEMO_PREWARM_BUDGET_S: float = float(
        os.getenv("EXPANSION_MEMO_PREWARM_BUDGET_S", "600")
    )
    # Max number of concurrent LLM calls during pre-warm. Each worker
    # opens its own DB session — no cross-thread session sharing. Setting
    # this to ``1`` reverts to strict sequential execution (the rollback
    # path; see ``_prewarm_decision_memos``). Values above 10 risk hitting
    # OpenAI tier-1 RPM limits on gpt-4o-mini.
    EXPANSION_MEMO_PREWARM_CONCURRENCY: int = int(
        os.getenv("EXPANSION_MEMO_PREWARM_CONCURRENCY", "5")
    )


settings = Settings()
