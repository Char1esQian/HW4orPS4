from app.hw4 import is_hw4_likely_model_y


def test_fremont_threshold_true() -> None:
    vin = "7SAYGDEE8RF789500"
    is_likely, reason = is_hw4_likely_model_y(vin)
    assert is_likely is True
    assert "Fremont" in reason


def test_fremont_threshold_false() -> None:
    vin = "7SAYGDEE8RF789499"
    is_likely, reason = is_hw4_likely_model_y(vin)
    assert is_likely is False
    assert "below" in reason.lower()


def test_austin_threshold_true() -> None:
    vin = "7SAYGDEE8RA131200"
    is_likely, reason = is_hw4_likely_model_y(vin)
    assert is_likely is True
    assert "Austin" in reason


def test_invalid_vin_non_numeric_serial() -> None:
    vin = "7SAYGDEE8RA12X20Z"
    is_likely, reason = is_hw4_likely_model_y(vin)
    assert is_likely is False
    assert "numeric" in reason.lower()


def test_missing_vin() -> None:
    is_likely, reason = is_hw4_likely_model_y(None)
    assert is_likely is False
    assert "missing" in reason.lower()

