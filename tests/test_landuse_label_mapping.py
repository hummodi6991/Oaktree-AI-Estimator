import pytest

from app.services.geo import _landuse_code_from_label


@pytest.mark.parametrize(
    "label,expected",
    [
        ("سكني", "s"),
        ("سكني فقط", "s"),
        ("سكني - فلل", "s"),
        ("سكني تجاري", "m"),
        ("تجاري سكني", "m"),
        ("مختلط", "m"),
        ("تجاري", "m"),
        ("residential", "s"),
        ("mixed", "m"),
        ("commercial", "m"),
    ],
)
def test_landuse_code_from_label(label, expected):
    assert _landuse_code_from_label(label) == expected
