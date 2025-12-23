from app.services.geo import _landuse_code_from_label


def test_mapper():
    assert _landuse_code_from_label("سكني") == "s"
    assert _landuse_code_from_label("residential") == "s"
    assert _landuse_code_from_label("house") == "s"
    assert _landuse_code_from_label("commercial") == "m"
    assert _landuse_code_from_label("Mixed-Use") is None
    assert _landuse_code_from_label("yes") is None
