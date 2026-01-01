from app.services.pricing_response import normalize_land_price_quote


def test_normalize_blended_shape_preserves_value():
    quote = {
        "provider": "blended_v1",
        "method": "blended_v1",
        "value": 1234.5,
        "district_raw": "Raw District",
        "district_norm": "Norm District",
        "district_resolution": {"method": "resolver"},
        "meta": {
            "components": {
                "suhail": {
                    "value": 1100.0,
                    "as_of_date": "2024-01-01",
                    "land_use_group": "الكل",
                    "last_txn_date": "2024-02-01",
                    "last_price_ppm2": 1050.0,
                    "level": "district",
                },
                "aqar": {
                    "value": 1300.0,
                    "n": 25,
                    "level": "district",
                },
            },
            "weights": {"suhail": 0.7, "aqar": 0.3},
            "guardrails": {"aqar_low_evidence": False},
        },
    }

    normalized = normalize_land_price_quote("Riyadh", "blended_v1", quote)

    assert normalized["value_sar_m2"] == quote["value"]
    assert normalized["district_norm"] == "Norm District"
    assert normalized["meta"]["components"]["suhail"]["value_sar_m2"] == 1100.0
    assert normalized["meta"]["components"]["aqar"]["value_sar_m2"] == 1300.0
    assert normalized["meta"]["components"]["hedonic"]["value_sar_m2"] is None
    assert normalized["meta"]["weights"] == {"suhail": 0.7, "aqar": 0.3, "hedonic": None}
    assert normalized["meta"]["guardrails"]["low_evidence"] is False


def test_normalize_suhail_fills_missing_components():
    quote = {
        "provider": "suhail",
        "value": 900.0,
        "method": "suhail_land_metrics_median",
        "district_norm": "resolved_district",
        "district_raw": "raw_district",
        "district_resolution": {"method": "resolver"},
        "meta": {
            "source": "suhail_land_metrics",
            "district_norm": "resolved_district",
            "district_resolution": {"method": "resolver"},
        },
    }

    normalized = normalize_land_price_quote("Riyadh", "suhail", quote)

    assert normalized["value_sar_m2"] == 900.0
    assert normalized["district_norm"] == "resolved_district"
    assert normalized["meta"]["components"]["suhail"]["value_sar_m2"] == 900.0
    assert normalized["meta"]["components"]["aqar"]["value_sar_m2"] is None
    assert normalized["meta"]["weights"] == {"suhail": None, "aqar": None, "hedonic": None}
    assert normalized["meta"]["guardrails"]["low_evidence"] is None


def test_normalize_kaggle_hedonic_sets_component_and_model_version():
    quote = {
        "provider": "kaggle_hedonic_v0",
        "value": 800.0,
        "method": "kaggle_hedonic_v0",
        "district_norm": "norm_district",
        "district_raw": "raw_district",
        "district_resolution": {"method": "resolver"},
        "meta": {
            "district_norm": "norm_district",
            "district_raw": "raw_district",
            "district_resolution": {"method": "resolver"},
            "hedonic_meta": {"version": "0.1.0", "model_used": True},
        },
    }

    normalized = normalize_land_price_quote("Riyadh", "kaggle_hedonic_v0", quote)

    assert normalized["value_sar_m2"] == 800.0
    assert normalized["district_raw"] == "raw_district"
    assert normalized["meta"]["components"]["hedonic"]["value_sar_m2"] == 800.0
    assert normalized["meta"]["components"]["hedonic"]["model_version"] == "0.1.0"
    assert normalized["meta"]["components"]["suhail"]["value_sar_m2"] is None
    assert normalized["meta"]["components"]["aqar"]["value_sar_m2"] is None
