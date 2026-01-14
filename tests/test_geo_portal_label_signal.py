from app.api.geo_portal import _label_is_signal


def test_label_is_signal_accepts_arabic_labels_with_codes():
    assert _label_is_signal("سكني", "s") is True
    assert _label_is_signal("مختلط", "m") is True


def test_label_is_signal_rejects_noisy_english_labels():
    assert _label_is_signal("building", "s") is False


def test_label_is_signal_allows_valid_english_labels():
    assert _label_is_signal("residential", "s") is True
