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
    # ≥3 contributing branches in the candidate's catchment, the snapshot
    # writer publishes ``realized_demand_30d`` / ``realized_demand_branches``
    # to feature_snapshot_json and the service layer blends a realized-demand
    # score (rating_count growth per category per radius over the last N
    # days) into the supply-based _delivery_score().  This is delivery rating
    # velocity — a partial proxy for orders, not an order count, since only a
    # fraction of orders produce a rating; it is surfaced to users as
    # "delivery rating velocity", never as "orders".
    #
    # Default flipped ON (B3): the production coverage check confirmed
    # ~3,128 of 7,405 recent candidates (trailing 90d) carry ≥3 branches in
    # the 1200 m catchment. The signal is now first-class, not opt-in. Set
    # EXPANSION_REALIZED_DEMAND_ENABLED=false to restore the legacy behavior
    # (snapshot fields suppressed and realized_demand_source reported as
    # ``history_unavailable``).
    EXPANSION_REALIZED_DEMAND_ENABLED: bool = (
        os.getenv("EXPANSION_REALIZED_DEMAND_ENABLED", "true").strip().lower()
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
    # Reference point for the square-root-scaled realized-demand score in
    # _delivery_score(): realized_demand == this value maps to a score of 100.
    # Calibrated 2026-05-15 from the trailing-90d realized_demand_30d
    # distribution across 3,128 populated candidates (median 133, p75 263,
    # p90 496, p95 840). Anchor at p75: a candidate at the 75th percentile of
    # measured demand saturates the demand leg, median candidates score ~71,
    # only the top quartile maxes out. See
    # scripts/diagnostics/realized_demand_calibration.sql.
    EXPANSION_REALIZED_DEMAND_REFERENCE: float = float(
        os.getenv("EXPANSION_REALIZED_DEMAND_REFERENCE", "263.0")
    )

    # --- Expansion Advisor L1 modeled demand-generator index (PR-1) ---
    # Additive, emit-only feature: builds a per-candidate demand-generator
    # index (catchment population + OSM trip generators + Overture building
    # floor-density + free review_count-weighted F&B density) and writes it
    # into feature_snapshot_json["demand_generator_index"] for validation.
    # It is a demand NUMERATOR only and is NOT read by scoring in PR-1.
    # Default OFF: when false the whole enrich path is inert and rankings /
    # feature_snapshot_json are byte-for-byte unchanged.
    EXPANSION_DEMAND_GENERATOR_INDEX_ENABLED: bool = (
        os.getenv("EXPANSION_DEMAND_GENERATOR_INDEX_ENABLED", "false").strip().lower()
        in {"1", "true", "yes", "on"}
    )
    # Dine-in demand catchment radius (metres). Single configurable constant so
    # validation can retune without a code change. Matches
    # _CATCHMENT_RADII_M["dine_in"]["demand"] (3500 m).
    EXPANSION_DEMAND_GENERATOR_RADIUS_M: int = int(
        os.getenv("EXPANSION_DEMAND_GENERATOR_RADIUS_M", "3500")
    )
    # Population sub-term catchment radius (metres) for the L1 index (PR-1a).
    # At the 3500 m demand radius the population term is near-constant in dense
    # Riyadh (~250k everywhere) and barely discriminates, so the index's
    # population SUB-SCORE is computed at a tighter radius where it actually
    # varies. The full 3500 m population_reach is still retained raw in the
    # snapshot for continuity. Default 1500 m.
    EXPANSION_DEMAND_GENERATOR_POP_RADIUS_M: int = int(
        os.getenv("EXPANSION_DEMAND_GENERATOR_POP_RADIUS_M", "1500")
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

    # --- Expansion Advisor market-viability conjunction pass (CEO directive #1) ---
    # Soft positional demotion for candidates that are confidently bad on BOTH
    # currently-measured legs of the directive: high rent percentile AND low
    # population reach. Mirrors _apply_value_band_pass mechanics (positional
    # reorder only, no final_score mutation). The third leg (growth) is not
    # enforced because the repo has no honest growth signal as of this patch.
    EXPANSION_VIABILITY_RENT_PCT_THRESHOLD: float = float(
        os.getenv("EXPANSION_VIABILITY_RENT_PCT_THRESHOLD", "0.70")
    )
    EXPANSION_VIABILITY_POP_PERCENTILE: float = float(
        os.getenv("EXPANSION_VIABILITY_POP_PERCENTILE", "0.25")
    )
    # Bottom-quartile cutoff for the realized-demand soft-demote leg
    # (clause 2 "strong potential for sales"). Mirrors
    # EXPANSION_VIABILITY_POP_PERCENTILE.
    EXPANSION_VIABILITY_DEMAND_PERCENTILE: float = float(
        os.getenv("EXPANSION_VIABILITY_DEMAND_PERCENTILE", "0.25")
    )
    # Minimum number of distinct delivery-platform branches with rating-
    # count delta capability inside the candidate's catchment for the
    # demand leg to consider the signal confident. Mirrors the snapshot-
    # writer gate at app/services/expansion_advisor.py:7933 and lets us
    # tighten/loosen the leg without touching the snapshot path.
    EXPANSION_VIABILITY_DEMAND_MIN_BRANCHES: int = int(
        os.getenv("EXPANSION_VIABILITY_DEMAND_MIN_BRANCHES", "3")
    )
    # Kill switch for the demand_demote leg only. Disabling this leaves
    # the realized-demand data pipeline (snapshots, memo phrasing, rerank
    # emission) fully intact — only the soft-demote behavior is suppressed.
    # Use when production ranking moves are problematic but the data is
    # still wanted for audit / observability.
    EXPANSION_VIABILITY_DEMAND_LEG_ENABLED: bool = (
        os.getenv("EXPANSION_VIABILITY_DEMAND_LEG_ENABLED", "true").strip().lower()
        in {"1", "true", "yes", "on"}
    )
    EXPANSION_VIABILITY_DEMOTION_STEPS: int = int(
        os.getenv("EXPANSION_VIABILITY_DEMOTION_STEPS", "6")
    )
    # rent_per_capita demote leg: catches the "low-pop + high-rent" anti-pattern
    # via a cohort percentile on estimated_annual_rent_sar / population_reach.
    # Mirrors EXPANSION_VIABILITY_POP_PERCENTILE (per-search cohort cutoff,
    # positional reorder, no final_score mutation). Top-quartile of cohort
    # gets demoted at the default 0.75.
    EXPANSION_VIABILITY_RPC_PERCENTILE: float = float(
        os.getenv("EXPANSION_VIABILITY_RPC_PERCENTILE", "0.75")
    )
    # Below this cohort size, the rpc leg is skipped entirely (avoids noisy
    # percentile cutoffs on tiny samples). No demotions and no flag writes
    # below this floor.
    EXPANSION_VIABILITY_RPC_MIN_COHORT: int = int(
        os.getenv("EXPANSION_VIABILITY_RPC_MIN_COHORT", "10")
    )
    # Threshold for the economics-quality leg of _apply_market_viability_pass
    # (CEO directive clause 3 — "strong potential for profitability"). Candidates
    # with economics_score < this threshold are soft-demoted by the same
    # positional mechanism as the rent and population legs. Calibrated against
    # the 30-day production cohort 2026-04-04 → 2026-05-04: 6.0% of candidates
    # would fail at 65.0; raise to tighten, lower to loosen. Setting this to
    # 999 effectively disables the leg (no candidate scores 999+).
    EXPANSION_VIABILITY_ECONOMICS_MIN: float = float(
        os.getenv("EXPANSION_VIABILITY_ECONOMICS_MIN", "65.0")
    )
    # Minimum YoY radiance growth pct (0-100 scale) for the third leg (NASA
    # Black Marble VNP46A3) to rescue a candidate from market-viability
    # flagging. Operator is ``>=``. Rescue threshold for "confident,
    # meaningfully growing" districts: only YoY at or above this floor
    # mutes the pop/rent demote legs. Recalibrated 2026-05-10 against the
    # rolling-6 candidate distribution (radiance_yoy_distribution.sql §7a):
    # of 1,960 confident candidates over the last 30 days, ~1,000 (~51%)
    # sit at >=2.0% YoY — the upper half of the confident-signal cohort,
    # which is the "meaningfully growing" tier we want to rescue. Holding
    # this at 0.0 rescued ~93% of confident candidates and made the
    # rescue meaningless.
    EXPANSION_VIABILITY_RADIANCE_YOY_THRESHOLD: float = float(
        os.getenv("EXPANSION_VIABILITY_RADIANCE_YOY_THRESHOLD", "2.0")
    )
    # Demote threshold for the radiance-growth leg of
    # _apply_market_viability_pass (CEO directive pillar 3 — "strong
    # potential for business growth"). When ``radiance_growth.confident``
    # is True and ``value_yoy_pct`` < this threshold, the candidate is
    # soft-demoted by the same positional mechanism as the population /
    # rent / economics / demand legs. Operator is strict ``<``.
    # Recalibrated 2026-05-10 against the rolling-6 candidate distribution
    # (radiance_yoy_distribution.sql §7b): of 1,960 confident candidates
    # over the last 30 days, only ~140 (~7.1%) sit below 0% YoY. The prior
    # 2.0 threshold demoted ~49% of confident candidates (820 flat 0..2%
    # + 140 shrinking), compressing top scores; 0.0 isolates the
    # "confidently shrinking" tier — districts whose own NTL signal is
    # contracting — which is the only group the demote leg should fire
    # on per Pillar 3. Distinct from
    # ``EXPANSION_VIABILITY_RADIANCE_YOY_THRESHOLD`` above (which drives
    # the rescue side, operator ``>=``); splitting the knobs prevents
    # calibrating one from silently affecting the other.
    EXPANSION_VIABILITY_RADIANCE_YOY_DEMOTE_THRESHOLD: float = float(
        os.getenv("EXPANSION_VIABILITY_RADIANCE_YOY_DEMOTE_THRESHOLD", "0.0")
    )
    # Kill switch for the radiance-growth demote leg. Setting this to
    # ``false`` suppresses the leg's demote decision while leaving the
    # ``radiance_growth`` snapshot field (and the advisory
    # ``radiance_growth_pass`` gate emission) fully intact — only the
    # soft-demote behavior is suppressed. The ``GROWTH`` disambiguator
    # is intentional: multiple Black Marble metrics may land later, and
    # the env var is the operator-facing name.
    EXPANSION_VIABILITY_RADIANCE_GROWTH_LEG_ENABLED: bool = (
        os.getenv("EXPANSION_VIABILITY_RADIANCE_GROWTH_LEG_ENABLED", "true").strip().lower()
        in {"1", "true", "yes", "on"}
    )
    # Hard floors (CEO directive — broader data + filter low-potential
    # locations). Unlike the soft demotion legs above, these are absolute
    # drops applied before the 3-of-3 conjunction. A value of 0 disables
    # the corresponding gate entirely (no candidate is dropped on that
    # leg). See ``_apply_market_viability_pass`` for evaluation order and
    # the "missing field → pass" semantics that protect historical rows.
    EXPANSION_VIABILITY_POPULATION_HARD_FLOOR: int = int(
        os.getenv("EXPANSION_VIABILITY_POPULATION_HARD_FLOOR", "20000")
    )
    EXPANSION_VIABILITY_BRAND_PRESENCE_HARD_FLOOR: int = int(
        os.getenv("EXPANSION_VIABILITY_BRAND_PRESENCE_HARD_FLOOR", "1")
    )
    # Construction-proximity hard floor (CEO directive — exclude areas with
    # heavy construction). Drops any candidate that has at least one
    # ``planet_osm_polygon`` row tagged ``landuse='construction'`` or
    # ``building='construction'`` within this radius (meters). 0 disables
    # the gate entirely (every candidate emits
    # ``construction_proximity_pass=True``). Same "missing field → pass"
    # semantics as the population/commercial floors above: when the OSM
    # bulk pre-compute is skipped (table missing, buffer disabled) or
    # the snapshot block is absent, the candidate passes defensively.
    EXPANSION_VIABILITY_CONSTRUCTION_BUFFER_M: float = float(
        os.getenv("EXPANSION_VIABILITY_CONSTRUCTION_BUFFER_M", "75.0")
    )
    # Centroid clip radius (km) used by ``_query_commercial_unit_candidates``
    # when a target district is named. 0 disables the clip entirely,
    # letting the search reach all of Riyadh. When no target district is
    # passed to the query, the clip is never applied regardless of this
    # value.
    EXPANSION_CENTROID_CLIP_KM: float = float(
        os.getenv("EXPANSION_CENTROID_CLIP_KM", "10.0")
    )

    # Weight (in percent) of chain_strength_score in the Expansion Advisor
    # scoring composite. Pro-presence direction — higher max
    # chain_strength_score (from expansion_competitor_quality) within a
    # candidate's competition radius raises its score, on the theory that
    # an established brand operating nearby is evidence the area is
    # validated by serious operators. Pulled from competition_whitespace
    # (8.7640 → 5.7640), so the 10 component weights still sum to 100.0.
    # Set to 0 to disable the chain_strength leg without code changes; if
    # changed, EXPANSION_COMPETITION_WHITESPACE_WEIGHT must be adjusted in
    # lockstep — the runtime assertion in _score_breakdown catches drift.
    EXPANSION_CHAIN_STRENGTH_WEIGHT: float = float(
        os.getenv("EXPANSION_CHAIN_STRENGTH_WEIGHT", "3.0")
    )

    # Strong-chain SHARE calibration for the chain_strength leg input.
    # Replaces the MAX-over-radius leg input (which saturated at 100 for any
    # radius containing a single big chain) with the SHARE of same-category,
    # ECQ-matched POIs in the radius whose chain_strength_score is "strong".
    # EXPANSION_CHAIN_STRONG_THRESHOLD: an ECQ chain_strength_score at/above
    #   this counts as a strong/established chain (default 60.0 ≈ a 5+ branch
    #   chain on the ingest ladder LEAST(100, chain_size*12)).
    # EXPANSION_CHAIN_MIN_MATCHED: minimum number of in-category ECQ-matched
    #   POIs required before a share is trustworthy; below it the leg input is
    #   NULL → Python None → _chain_strength_score keeps the neutral 50.0.
    EXPANSION_CHAIN_STRONG_THRESHOLD: float = float(
        os.getenv("EXPANSION_CHAIN_STRONG_THRESHOLD", "60.0")
    )
    EXPANSION_CHAIN_MIN_MATCHED: int = int(
        os.getenv("EXPANSION_CHAIN_MIN_MATCHED", "3")
    )

    # --- Expansion Advisor brand-weight reweighting (Finding 1) ---
    # Brand-brief soft knobs (parking/frontage/visibility sensitivity, primary_channel,
    # expansion_goal) re-weight the top-level component_weights in _score_breakdown
    # instead of only nudging terms inside brand_fit (9.64% of final_score). The gain
    # scales how strongly a "high"/"low" knob moves its target component weight before
    # renormalization to 100. 0.0 disables the reweighting entirely (every multiplier
    # becomes 1.0 → byte-identical to the pre-Finding-1 static weights).
    EXPANSION_BRAND_WEIGHT_GAIN: float = float(
        os.getenv("EXPANSION_BRAND_WEIGHT_GAIN", "0.35")
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
