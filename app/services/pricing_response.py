from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, Optional


def _extract_first_value(*sources: Any) -> Optional[float]:
    for src in sources:
        if src is None:
            continue
        if isinstance(src, dict):
            for key in ("value", "value_sar_m2", "ppm2", "median_ppm2", "sar_per_m2"):
                if src.get(key) is not None:
                    return src[key]
        else:
            return src
    return None


def _normalize_suhail_component(data: Dict[str, Any] | None, fallback_value: Optional[float]) -> Dict[str, Any]:
    base = {
        "value_sar_m2": None,
        "as_of_date": None,
        "land_use_group": None,
        "last_txn_date": None,
        "last_price_sar_m2": None,
        "level": None,
    }
    if isinstance(data, dict):
        base["value_sar_m2"] = _extract_first_value(data)
        base["as_of_date"] = data.get("as_of_date")
        base["land_use_group"] = data.get("land_use_group")
        base["last_txn_date"] = data.get("last_txn_date")
        base["last_price_sar_m2"] = data.get("last_price_sar_m2") or data.get("last_price_ppm2")
        base["level"] = data.get("level")

    if base["value_sar_m2"] is None:
        base["value_sar_m2"] = fallback_value
    return base


def _normalize_aqar_component(data: Dict[str, Any] | None, fallback_value: Optional[float]) -> Dict[str, Any]:
    base = {
        "value_sar_m2": None,
        "n": None,
        "level": None,
    }
    if isinstance(data, dict):
        base["value_sar_m2"] = _extract_first_value(data)
        base["n"] = data.get("n")
        base["level"] = data.get("level")

    if base["value_sar_m2"] is None:
        base["value_sar_m2"] = fallback_value
    return base


def _extract_model_version(meta: Dict[str, Any]) -> Optional[str]:
    for key in ("model_version", "version", "model_name", "name"):
        if key in meta and meta.get(key) is not None:
            return str(meta.get(key))
    model_meta = meta.get("model")
    if isinstance(model_meta, dict):
        for key in ("model_version", "version", "model_name", "name"):
            if key in model_meta and model_meta.get(key) is not None:
                return str(model_meta.get(key))
    if isinstance(model_meta, str):
        return model_meta
    return None


def _normalize_hedonic_component(data: Dict[str, Any] | None, fallback_value: Optional[float]) -> Dict[str, Any]:
    base = {
        "value_sar_m2": None,
        "model_version": None,
    }
    if isinstance(data, dict):
        base["value_sar_m2"] = _extract_first_value(data)
        base["model_version"] = _extract_model_version(data)

    if base["value_sar_m2"] is None:
        base["value_sar_m2"] = fallback_value
    return base


def normalize_land_price_quote(
    city: str,
    provider: str | None,
    raw_quote: Dict[str, Any] | None,
    fallback_method: str | None = None,
) -> Dict[str, Any]:
    """
    Normalize provider-specific land price outputs into a canonical schema.
    """
    quote = raw_quote or {}
    raw_meta = quote.get("meta") if isinstance(quote.get("meta"), dict) else {}
    provider_key = (provider or quote.get("provider") or "kaggle_hedonic_v0").lower()
    method = quote.get("method") or fallback_method or provider_key

    value_sar_m2 = _extract_first_value(quote, raw_meta)
    district_raw = (
        quote.get("district_raw")
        or quote.get("district")
        or raw_meta.get("district_raw")
        or raw_meta.get("district")
    )
    district_norm = quote.get("district_norm") or raw_meta.get("district_norm")
    district_resolution = quote.get("district_resolution") or raw_meta.get("district_resolution")
    if not isinstance(district_resolution, dict):
        district_resolution = None

    components_meta = raw_meta.get("components") if isinstance(raw_meta, dict) else {}
    if not isinstance(components_meta, dict):
        components_meta = {}

    # Shape component data inputs
    suhail_component = components_meta.get("suhail")
    if provider_key == "suhail" and suhail_component is None:
        base_meta = deepcopy(raw_meta) if isinstance(raw_meta, dict) else {}
        base_meta["value"] = value_sar_m2
        suhail_component = base_meta

    aqar_component = components_meta.get("aqar")
    hedonic_component = components_meta.get("hedonic") or raw_meta.get("hedonic_meta")
    if provider_key in {"kaggle_hedonic_v0", "hedonic"} and hedonic_component is None:
        hedonic_component = deepcopy(raw_meta) if isinstance(raw_meta, dict) else {}
        hedonic_component["value"] = value_sar_m2

    meta_components = {
        "suhail": _normalize_suhail_component(suhail_component, value_sar_m2 if provider_key == "suhail" else None),
        "aqar": _normalize_aqar_component(aqar_component, value_sar_m2 if provider_key == "aqar_median" else None),
        "hedonic": _normalize_hedonic_component(
            hedonic_component, value_sar_m2 if provider_key in {"kaggle_hedonic_v0", "hedonic"} else None
        ),
    }

    weights_meta = raw_meta.get("weights") if isinstance(raw_meta, dict) else {}
    weights = {
        "suhail": weights_meta.get("suhail") if isinstance(weights_meta, dict) else None,
        "aqar": weights_meta.get("aqar") if isinstance(weights_meta, dict) else None,
        "hedonic": weights_meta.get("hedonic") if isinstance(weights_meta, dict) else None,
    }

    guardrails_meta = raw_meta.get("guardrails") if isinstance(raw_meta, dict) else {}
    guardrails = {
        "low_evidence": None,
        "disagreement": None,
        "clamped": None,
    }
    if isinstance(guardrails_meta, dict):
        guardrails["low_evidence"] = guardrails_meta.get("low_evidence", guardrails_meta.get("aqar_low_evidence"))
        guardrails["disagreement"] = guardrails_meta.get("disagreement")
        guardrails["clamped"] = guardrails_meta.get("clamped")

    notes: Any = deepcopy(raw_meta) if raw_meta else None

    return {
        "city": city,
        "provider": provider_key,
        "method": method,
        "value_sar_m2": value_sar_m2,
        "district_raw": district_raw,
        "district_norm": district_norm,
        "district_resolution": district_resolution,
        "meta": {
            "components": meta_components,
            "weights": weights,
            "guardrails": guardrails,
            "notes": notes,
        },
    }
