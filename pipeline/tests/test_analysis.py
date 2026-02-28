from datetime import date

import pandas as pd
import pytest

from pipeline.reporting.analysis import (
    classify_state_from_score,
    latest_pct_change,
    normalize_base100,
)


def test_normalize_base100_uses_first_valid_after_start_date():
    index = pd.to_datetime(["2025-06-30", "2025-07-01", "2025-07-02"])
    frame = pd.DataFrame({"A": [90.0, 100.0, 110.0]}, index=index)

    normalized = normalize_base100(frame, date(2025, 7, 1))

    assert normalized.loc[pd.Timestamp("2025-07-01"), "A"] == pytest.approx(100.0)
    assert normalized.loc[pd.Timestamp("2025-07-02"), "A"] == pytest.approx(110.0)


def test_latest_pct_change_returns_zero_for_short_series():
    series = pd.Series([100.0])
    assert latest_pct_change(series) == 0.0


def test_classify_state_from_score_boundaries():
    assert classify_state_from_score(1.2) == ("강세", "bullish")
    assert classify_state_from_score(-1.1) == ("약세", "bearish")
    assert classify_state_from_score(0.2) == ("중립", "neutral")
