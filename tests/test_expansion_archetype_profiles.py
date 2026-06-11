"""Brand archetypes (EXPANSION_ARCHETYPE_PROFILES) — legible weight profiles.

Contract points:
  * flag off (default) ⇒ byte-identical weights/contributions to the static
    v1/v2 stacks, even with a brand_archetype present in the profile;
  * resolution order: explicit brand_archetype > legacy non-default
    expansion_goal > service_model seed;
  * flag on + v2: each archetype's applied weights match the locked profiles
    exactly; knob multipliers compose on top and renormalize to 100;
  * flag on + v2: channel/goal weight-multiplier roles are retired; the
    max() asymmetry on the three sensitivities is fixed (single "low" moves
    access_visibility down);
  * flag on + v1: archetypes are ignored (log-once), v1 weights unchanged;
  * brand_fit's goal_component keys off the resolved archetype when the
    flag is on.
"""

from __future__ import annotations

import pytest

from app.services import expansion_advisor as expansion_service
from app.services.expansion_advisor import (
    _ARCHETYPE_WEIGHT_PROFILES,
    BRAND_ARCHETYPES,
    resolve_brand_archetype,
)


def _breakdown_kwargs(**overrides):
    kwargs = dict(
        demand_score=80,
        whitespace_score=70,
        brand_fit_score=75,
        economics_score=60,
        provider_intelligence_composite=65,
        access_visibility_score=55,
        confidence_score=50,
        listing_quality_score=60,
        landlord_signal_score=40,
        chain_strength_score=30,
    )
    kwargs.update(overrides)
    return kwargs


def _set(monkeypatch, name: str, value) -> None:
    monkeypatch.setattr(expansion_service.settings, name, value, raising=False)


def _archetypes_on(monkeypatch, gain: float = 0.35) -> None:
    _set(monkeypatch, "EXPANSION_WEIGHT_STACK", "v2")
    _set(monkeypatch, "EXPANSION_ARCHETYPE_PROFILES", True)
    _set(monkeypatch, "EXPANSION_BRAND_WEIGHT_GAIN", gain)


V2_WEIGHTS = {
    "occupancy_economics": 20.0,
    "demand_potential": 18.0,
    "competition_whitespace": 12.0,
    "access_visibility": 11.0,
    "listing_quality": 9.0,
    "brand_fit": 8.0,
    "district_momentum": 7.0,
    "delivery_demand": 6.0,
    "landlord_signal": 5.0,
    "chain_strength": 4.0,
}


# ── Locked weight profiles (L2) ──────────────────────────────────────


def test_archetype_profiles_sum_to_100_and_guard_brand_fit():
    assert set(_ARCHETYPE_WEIGHT_PROFILES.keys()) == set(BRAND_ARCHETYPES)
    for name, profile in _ARCHETYPE_WEIGHT_PROFILES.items():
        assert sum(profile.values()) == 100.0, name
        assert set(profile.keys()) == set(V2_WEIGHTS.keys()), name
        # Pathology guard: brand_fit's demand-inverse rank dominance means
        # no profile may lift it above the v2 baseline of 8.
        assert profile["brand_fit"] <= 8.0, name
    assert _ARCHETYPE_WEIGHT_PROFILES["balanced"] == V2_WEIGHTS


# ── Resolution order (L3 / L5) ───────────────────────────────────────


def test_resolution_explicit_beats_seeded():
    assert (
        resolve_brand_archetype({"brand_archetype": "street_flagship"}, "cafe")
        == "street_flagship"
    )
    assert resolve_brand_archetype({"brand_archetype": "balanced"}, "cafe") == "balanced"


@pytest.mark.parametrize(
    "service_model, expected",
    [
        ("qsr", "balanced"),
        ("delivery_first", "delivery_led"),
        ("cafe", "neighborhood_local"),
        ("dine_in", "balanced"),
        ("unknown_model", "balanced"),
        (None, "balanced"),
    ],
)
def test_resolution_seeds_from_service_model(service_model, expected):
    assert resolve_brand_archetype(None, service_model) == expected
    assert resolve_brand_archetype({}, service_model) == expected


@pytest.mark.parametrize(
    "goal, expected",
    [
        ("flagship", "street_flagship"),
        ("neighborhood", "neighborhood_local"),
        ("delivery_led", "delivery_led"),
    ],
)
def test_resolution_legacy_goal_maps(goal, expected):
    # Non-default legacy goals beat the service_model seed.
    assert resolve_brand_archetype({"expansion_goal": goal}, "cafe") == expected


def test_resolution_legacy_balanced_goal_defers_to_seed():
    # "balanced" is the _default_brand_profile fill — indistinguishable from
    # an untouched knob, so the service_model seed wins.
    assert (
        resolve_brand_archetype({"expansion_goal": "balanced"}, "cafe")
        == "neighborhood_local"
    )


def test_resolution_explicit_beats_legacy_goal():
    assert (
        resolve_brand_archetype(
            {"brand_archetype": "balanced", "expansion_goal": "flagship"}, "qsr"
        )
        == "balanced"
    )


def test_run_search_default_profile_carries_resolved_archetype():
    profile = expansion_service._default_brand_profile(None)
    assert profile["brand_archetype"] is None
    assert (
        resolve_brand_archetype(profile, "delivery_first") == "delivery_led"
    )


# ── Flag-off inertness (L4) ──────────────────────────────────────────


def test_flag_off_v2_byte_identical_with_archetype_present(monkeypatch):
    assert expansion_service.settings.EXPANSION_ARCHETYPE_PROFILES is False
    _set(monkeypatch, "EXPANSION_WEIGHT_STACK", "v2")
    _set(monkeypatch, "EXPANSION_BRAND_WEIGHT_GAIN", 0.35)

    baseline = expansion_service._score_breakdown(**_breakdown_kwargs())
    with_archetype = expansion_service._score_breakdown(
        **_breakdown_kwargs(),
        brand_profile={"brand_archetype": "street_flagship"},
        service_model="qsr",
    )
    assert with_archetype == baseline
    assert baseline["weights"] == V2_WEIGHTS
    assert "brand_archetype" not in baseline

    # Knob behavior is unchanged flag-off — including the legacy max()
    # asymmetry: a single "low" sensitivity stays a no-op.
    single_low = expansion_service._score_breakdown(
        **_breakdown_kwargs(),
        brand_profile={"parking_sensitivity": "low"},
        service_model="qsr",
    )
    assert single_low["weights"] == baseline["weights"]


def test_flag_off_v1_byte_identical_with_archetype_present(monkeypatch):
    _set(monkeypatch, "EXPANSION_BRAND_WEIGHT_GAIN", 0.35)
    baseline = expansion_service._score_breakdown(**_breakdown_kwargs())
    with_archetype = expansion_service._score_breakdown(
        **_breakdown_kwargs(),
        brand_profile={"brand_archetype": "delivery_led"},
        service_model="qsr",
    )
    assert with_archetype == baseline


# ── Flag-on weight application (L2 / L5) ─────────────────────────────


@pytest.mark.parametrize("archetype", BRAND_ARCHETYPES)
def test_flag_on_applies_locked_profile_exactly(monkeypatch, archetype):
    _archetypes_on(monkeypatch)
    breakdown = expansion_service._score_breakdown(
        **_breakdown_kwargs(),
        brand_profile={"brand_archetype": archetype},
        service_model="qsr",
    )
    assert breakdown["weights"] == _ARCHETYPE_WEIGHT_PROFILES[archetype]
    assert sum(breakdown["weights"].values()) == 100.0
    assert breakdown["brand_archetype"] == archetype


def test_flag_on_seeds_archetype_from_service_model(monkeypatch):
    _archetypes_on(monkeypatch)
    breakdown = expansion_service._score_breakdown(
        **_breakdown_kwargs(),
        brand_profile={},
        service_model="delivery_first",
    )
    assert breakdown["weights"] == _ARCHETYPE_WEIGHT_PROFILES["delivery_led"]
    assert breakdown["brand_archetype"] == "delivery_led"

    # No profile at all still seeds (run_expansion_search passes the filled
    # default profile, but _score_breakdown must not depend on that).
    none_profile = expansion_service._score_breakdown(
        **_breakdown_kwargs(),
        brand_profile=None,
        service_model="cafe",
    )
    assert none_profile["weights"] == _ARCHETYPE_WEIGHT_PROFILES["neighborhood_local"]


def test_flag_on_legacy_goal_maps_to_archetype_profile(monkeypatch):
    _archetypes_on(monkeypatch)
    breakdown = expansion_service._score_breakdown(
        **_breakdown_kwargs(),
        brand_profile={"expansion_goal": "flagship"},
        service_model="qsr",
    )
    assert breakdown["weights"] == _ARCHETYPE_WEIGHT_PROFILES["street_flagship"]


def test_flag_on_knob_multipliers_compose_and_renormalize(monkeypatch):
    _archetypes_on(monkeypatch)
    profile = {"brand_archetype": "street_flagship", "parking_sensitivity": "high"}
    weights = expansion_service._score_breakdown(
        **_breakdown_kwargs(), brand_profile=profile, service_model="qsr"
    )["weights"]
    # high parking (signal +1.0, gain 0.35): 17 × 1.35 renormalized over
    # 105.95 total → 21.6612.
    assert weights["access_visibility"] == pytest.approx(
        round(17.0 * 1.35 * 100.0 / 105.95, 4), abs=1e-4
    )
    assert weights["access_visibility"] > 17.0
    assert abs(sum(weights.values()) - 100) < 1e-3


def test_flag_on_single_low_sensitivity_trims_access_visibility(monkeypatch):
    """The legacy max() asymmetry is fixed in archetype mode: one "low" knob
    (signal −0.75) is no longer masked by the two neutral "medium" knobs."""
    _archetypes_on(monkeypatch)
    weights = expansion_service._score_breakdown(
        **_breakdown_kwargs(),
        brand_profile={"brand_archetype": "balanced", "parking_sensitivity": "low"},
        service_model="qsr",
    )["weights"]
    # low (signal −0.75, gain 0.35): 11 × 0.7375 renormalized over 97.1125
    # total → 8.3539.
    assert weights["access_visibility"] == pytest.approx(
        round(11.0 * 0.7375 * 100.0 / 97.1125, 4), abs=1e-4
    )
    assert weights["access_visibility"] < 11.0
    assert abs(sum(weights.values()) - 100) < 1e-3


def test_flag_on_high_beats_low_on_signal_magnitude(monkeypatch):
    _archetypes_on(monkeypatch)
    weights = expansion_service._score_breakdown(
        **_breakdown_kwargs(),
        brand_profile={
            "brand_archetype": "balanced",
            "parking_sensitivity": "low",
            "visibility_sensitivity": "high",
        },
        service_model="qsr",
    )["weights"]
    # |+1.0| > |−0.75| → the high knob wins.
    assert weights["access_visibility"] > 11.0


def test_flag_on_channel_and_goal_multipliers_retired(monkeypatch):
    _archetypes_on(monkeypatch)
    weights = expansion_service._score_breakdown(
        **_breakdown_kwargs(),
        brand_profile={"brand_archetype": "balanced", "primary_channel": "delivery"},
        service_model="qsr",
    )["weights"]
    # primary_channel no longer lifts delivery_demand/competition_whitespace
    # in the weight domain (its gate and _channel_fit_score roles persist).
    assert weights == _ARCHETYPE_WEIGHT_PROFILES["balanced"]


def test_flag_on_under_v1_is_ignored(monkeypatch):
    _set(monkeypatch, "EXPANSION_ARCHETYPE_PROFILES", True)
    _set(monkeypatch, "EXPANSION_BRAND_WEIGHT_GAIN", 0.35)
    assert expansion_service.settings.EXPANSION_WEIGHT_STACK == "v1"
    baseline = expansion_service._score_breakdown(**_breakdown_kwargs())
    with_archetype = expansion_service._score_breakdown(
        **_breakdown_kwargs(),
        brand_profile={"brand_archetype": "street_flagship"},
        service_model="qsr",
    )
    assert with_archetype == baseline
    assert "district_momentum" not in with_archetype["weights"]


# ── brand_fit goal_component keyed on archetype (L5) ─────────────────


def _brand_fit_kwargs(profile):
    return dict(
        district="Olaya",
        area_m2=350.0,
        demand_score=70.0,
        fit_score=65.0,
        cannibalization_score=40.0,
        provider_density_score=55.0,
        provider_whitespace_score=60.0,
        multi_platform_presence_score=50.0,
        delivery_competition_score=45.0,
        visibility_signal=72.0,
        parking_signal=68.0,
        brand_profile=profile,
        service_model="qsr",
        target_area_m2=350.0,
    )


def test_brand_fit_goal_component_reads_archetype(monkeypatch):
    # Flag off, legacy goal "flagship".
    legacy = expansion_service._brand_fit_score(
        **_brand_fit_kwargs({"expansion_goal": "flagship"})
    )
    # Flag on, archetype street_flagship (legacy goal at its silent default):
    # the same flagship branch must fire, keyed on the archetype.
    _archetypes_on(monkeypatch)
    archetype = expansion_service._brand_fit_score(
        **_brand_fit_kwargs(
            {"brand_archetype": "street_flagship", "expansion_goal": "balanced"}
        )
    )
    assert archetype == legacy

    # And the legacy goal knob no longer drives the branch when the flag is
    # on — a delivery_led goal with an explicit balanced archetype scores
    # the balanced branch.
    explicit_balanced = expansion_service._brand_fit_score(
        **_brand_fit_kwargs(
            {"brand_archetype": "balanced", "expansion_goal": "delivery_led"}
        )
    )
    monkeypatch.setattr(
        expansion_service.settings,
        "EXPANSION_ARCHETYPE_PROFILES",
        False,
        raising=False,
    )
    flag_off_balanced = expansion_service._brand_fit_score(
        **_brand_fit_kwargs({"expansion_goal": "balanced"})
    )
    assert explicit_balanced == flag_off_balanced


# ── API resolution + persistence (L3) ────────────────────────────────


def test_api_persists_resolved_archetype(monkeypatch):
    from fastapi.testclient import TestClient

    from app.api import expansion_advisor as expansion_api
    from app.db.deps import get_db
    from app.main import app

    class DummyDB:
        def execute(self, stmt, params=None):
            return None

        def commit(self):
            pass

        def rollback(self):
            pass

        def close(self):
            pass

    persisted: dict = {}
    monkeypatch.setattr(
        expansion_api,
        "persist_existing_branches",
        lambda _db, _sid, _branches: None,
    )
    monkeypatch.setattr(
        expansion_api,
        "persist_brand_profile",
        lambda _db, _sid, profile: persisted.update(profile),
    )
    monkeypatch.setattr(
        expansion_api, "run_expansion_search", lambda **kwargs: []
    )

    def override_get_db():
        yield DummyDB()

    app.dependency_overrides[get_db] = override_get_db
    client = TestClient(app, raise_server_exceptions=False)
    try:
        # No explicit archetype: seeded from service_model.
        response = client.post(
            "/v1/expansion-advisor/searches",
            json={
                "brand_name": "Brand X",
                "category": "burger",
                "service_model": "delivery_first",
                "brand_profile": {"price_tier": "mid"},
            },
        )
        assert response.status_code == 200
        assert persisted["brand_archetype"] == "delivery_led"
        assert (
            response.json()["brand_profile"]["brand_profile"]["brand_archetype"]
            == "delivery_led"
        )

        # Explicit archetype wins over the seed.
        persisted.clear()
        response = client.post(
            "/v1/expansion-advisor/searches",
            json={
                "brand_name": "Brand X",
                "category": "burger",
                "service_model": "delivery_first",
                "brand_profile": {"brand_archetype": "street_flagship"},
            },
        )
        assert response.status_code == 200
        assert persisted["brand_archetype"] == "street_flagship"
    finally:
        app.dependency_overrides.pop(get_db, None)


def test_api_rejects_unknown_archetype(monkeypatch):
    from fastapi.testclient import TestClient

    from app.main import app

    client = TestClient(app, raise_server_exceptions=False)
    response = client.post(
        "/v1/expansion-advisor/searches",
        json={
            "brand_name": "Brand X",
            "category": "burger",
            "service_model": "qsr",
            "brand_profile": {"brand_archetype": "mall_kiosk"},
        },
    )
    assert response.status_code == 422
