"""Tests for the Estimator PDF EN/AR label table (PR-5a).

Mirrors the Expansion Advisor i18n parity tests. Guards three things:

  1. Parity: every token has a non-empty ``en`` and ``ar`` value.
  2. Graceful fallback: ``t`` never raises, falls back to ``en``, and passes
     unknown tokens through unchanged.
  3. EN byte-identity: every ``en`` value equals the literal that was hardcoded
     in ``app/services/pdf.py`` before PR-5a, so routing labels through
     ``t(token, "en")`` keeps the English PDF byte-for-byte identical.
  4. Arabic byte hygiene: yeh is U+064A and heh is U+0647; no Farsi yeh
     (U+06CC) or heh-doachashmee (U+06BE).
"""

import pytest

from app.services.estimator_i18n import LABELS, t, source_type_label


# ── 1. Parity ───────────────────────────────────────────────────────
@pytest.mark.parametrize("token", sorted(LABELS.keys()))
def test_every_token_has_nonempty_en(token: str) -> None:
    assert LABELS[token].get("en"), token


@pytest.mark.parametrize("token", sorted(LABELS.keys()))
def test_every_token_has_nonempty_ar(token: str) -> None:
    assert LABELS[token].get("ar"), token


# ── 2. Fallback behavior ────────────────────────────────────────────
def test_t_resolves_requested_lang() -> None:
    assert t("land_cost", "ar") == LABELS["land_cost"]["ar"]
    assert t("land_cost", "en") == LABELS["land_cost"]["en"]


def test_t_falls_back_to_en_for_unknown_lang() -> None:
    assert t("land_cost", "fr") == LABELS["land_cost"]["en"]


def test_t_passes_through_unknown_token() -> None:
    assert t("__does_not_exist__", "ar") == "__does_not_exist__"


def test_source_type_label_translates_known_and_passes_unknown() -> None:
    assert source_type_label("GASTAT", "ar") == LABELS["source_type.GASTAT"]["ar"]
    # EN is identity for known values (byte-identity guarantee).
    assert source_type_label("GASTAT", "en") == "GASTAT"
    # Unknown source types pass through unchanged in both langs.
    assert source_type_label("Bespoke", "ar") == "Bespoke"
    assert source_type_label("", "ar") == ""


# ── 3. EN byte-identity lock (vs the pre-PR-5a pdf.py literals) ──────
EXPECTED_EN = {
    "doc_title_prefix": "Estimate",
    "totals_section": "Totals (SAR)",
    "cost_breakdown_section": "Cost breakdown",
    "revenue_breakdown_section": "Revenue breakdown",
    "parking_summary_section": "Parking summary",
    "executive_summary_section": "Executive summary",
    "key_assumptions_section": "Key assumptions",
    "appendix_calc_trace_section": "Appendix: calculation trace",
    "appendix_top_comps_section": "Appendix: top comps",
    "land_value": "Land value",
    "total_capex": "Total capex",
    "annual_net_revenue": "Annual net revenue",
    "annual_noi": "Annual NOI",
    "unlevered_roi": "Unlevered ROI",
    "header_item": "Item",
    "header_amount": "Amount",
    "header_how_calculated": "How calculated",
    "header_assumption": "Assumption",
    "header_value": "Value",
    "header_source": "Source",
    "header_detail": "Detail",
    "header_id": "ID",
    "header_date": "Date",
    "header_location": "Location",
    "header_price_sar_m2": "Price (SAR/m2)",
    "effective_far_above_ground": "Effective FAR (above-ground)",
    "residential_bua": "Residential BUA",
    "retail_bua": "Retail BUA",
    "office_bua": "Office BUA",
    "basement_bua": "Basement BUA",
    "upper_annex_non_far_bua": "Upper annex (non-FAR, +0.5 floor)",
    "land_cost": "Land cost",
    "construction_direct": "Construction (direct)",
    "upper_annex_non_far_cost": "Upper annex construction cost (non-FAR)",
    "fitout": "Fit-out",
    "contingency": "Contingency",
    "consultants": "Consultants",
    "feasibility_fee": "Feasibility fee",
    "transaction_costs": "Transaction costs",
    "annual_net_income": "Annual net income",
    "opex": "OPEX",
    "rev_nla_rent_rate": "NLA * rent rate",
    "rev_sum_income_components": "Sum of income components",
    "rev_effective_suffix": "effective",
    "rev_of_annual_net_income": "of annual net income",
    "rev_annual_net_income_minus_opex": "Annual net income - OPEX",
    "rev_noi_over_capex": "NOI / total capex",
    "parking_required_spaces": "Required spaces",
    "parking_provided_spaces": "Provided spaces",
    "parking_deficit": "Deficit",
    "parking_compliant": "Compliant",
    "yes": "Yes",
    "no": "No",
    "na": "N/A",
    "no_assumptions_available": "No assumptions available.",
    "far_model_prior": "FAR (model prior)",
    "income_prefix": "Income:",
    "source_type.Model": "Model",
    "source_type.Observed": "Observed",
    "source_type.GASTAT": "GASTAT",
    "source_type.Riyadh Municipality": "Riyadh Municipality",
    "source_type.Derived": "Derived",
    "source_type.Assumption": "Assumption",
    "source_type.Manual": "Manual",
}


def test_en_values_match_pre_pr5a_literals() -> None:
    # Every label routed through t(...,"en") must equal the prior hardcoded
    # literal — this is the EN byte-identity contract.
    for token, expected in EXPECTED_EN.items():
        assert t(token, "en") == expected, token


def test_no_unexpected_tokens_without_en_lock() -> None:
    # If a token is added, add it to EXPECTED_EN too, so EN byte-identity
    # stays explicitly asserted.
    assert set(LABELS.keys()) == set(EXPECTED_EN.keys())


# ── 4. Arabic byte hygiene ──────────────────────────────────────────
@pytest.mark.parametrize("token", sorted(LABELS.keys()))
def test_ar_byte_hygiene(token: str) -> None:
    ar = LABELS[token]["ar"]
    assert "ی" not in ar, f"{token}: Farsi yeh U+06CC present"
    assert "ھ" not in ar, f"{token}: heh-doachashmee U+06BE present"
