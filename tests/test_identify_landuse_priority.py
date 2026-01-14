from app.api.geo_portal import _select_identify_landuse


def test_identify_priority_prefers_parcel_label():
    selection = _select_identify_landuse(
        label_raw="residential",
        label_code="s",
        label_is_signal=True,
        suhail_raw="سكني",
        suhail_code="s",
        suhail_signal=True,
        osm_code="m",
        osm_res=0.1,
        osm_com=0.9,
        osm_conf=0.8,
    )

    assert selection["landuse_code"] == "s"
    assert selection["landuse_method"] == "parcel_label"
    assert selection["residential_share"] is None
    assert selection["commercial_share"] is None
    assert selection["osm_conf"] == 0.0


def test_identify_priority_uses_suhail_when_label_weak():
    selection = _select_identify_landuse(
        label_raw="building",
        label_code=None,
        label_is_signal=False,
        suhail_raw="سكني",
        suhail_code="s",
        suhail_signal=True,
        osm_code="m",
        osm_res=0.2,
        osm_com=0.7,
        osm_conf=0.8,
    )

    assert selection["landuse_raw"] == "سكني"
    assert selection["landuse_code"] == "s"
    assert selection["landuse_method"] == "suhail_overlay"
    assert selection["residential_share"] is None
    assert selection["commercial_share"] is None


def test_identify_priority_falls_back_to_osm():
    selection = _select_identify_landuse(
        label_raw="building",
        label_code=None,
        label_is_signal=False,
        suhail_raw="",
        suhail_code=None,
        suhail_signal=False,
        osm_code="m",
        osm_res=0.2,
        osm_com=0.7,
        osm_conf=0.8,
    )

    assert selection["landuse_code"] == "m"
    assert selection["landuse_method"] == "osm_overlay"
    assert selection["residential_share"] == 0.2
    assert selection["commercial_share"] == 0.7


def test_identify_priority_never_returns_overture_overlay():
    selection = _select_identify_landuse(
        label_raw="building",
        label_code=None,
        label_is_signal=False,
        suhail_raw="",
        suhail_code=None,
        suhail_signal=False,
        osm_code=None,
        osm_res=0.0,
        osm_com=0.0,
        osm_conf=0.0,
    )

    assert selection["landuse_method"] != "overture_overlay"
