"""
Tests for rolling_roi.py. Run with:  python3 test_rolling_roi.py
"""
import itertools
import time

import fees
import rolling_roi

NOW = 1_800_000_000.0
DAY = 86400.0


def fee_pct(price):
    return fees.fee_pct_of_stake(price, category="sports") * 100.0


def bet(wallet, days_ago, result, price=0.5, stake_pct=100, resolved=True):
    return {
        "wallet": wallet,
        "alert_ts": NOW - days_ago * DAY,
        "resolved": resolved,
        "result": result,
        "current_price_at_alert": price,
        "stake_pct": stake_pct,
    }


def test_window_and_filters():
    bets = [
        bet("0xa", 1, "WIN"),
        bet("0xa", 13.9, "LOSS"),
        bet("0xa", 14.5, "WIN"),                      # outside the 14d window
        bet("0xa", 2, "WIN", resolved=False),         # unresolved
        bet("0xa", 2, None),                          # no result
        bet("0xa", 2, "WIN", price=0.99),             # unpriced / out of range
        bet("0xb", 3, "LOSS"),
    ]
    out = rolling_roi.compute_rolling_roi(bets, now_ts=NOW, window_days=14)
    assert out["0xa"]["n"] == 2, out["0xa"]
    assert out["0xa"]["wins"] == 1 and out["0xa"]["losses"] == 1
    assert out["0xb"]["n"] == 1
    # 50c win = +1u, loss = -1u -> 0% gross
    assert abs(out["0xa"]["roi_gross"]) < 1e-9


def test_net_of_fees_hand_calc():
    # two bets at 50c: one win, one loss, equal weight. gross ROI 0.
    # fee at 50c = 5% * (1 - 0.5) = 2.5% of stake on every bet -> net -2.5%
    bets = [bet("0xa", 1, "WIN", 0.5), bet("0xa", 2, "LOSS", 0.5)]
    out = rolling_roi.compute_rolling_roi(bets, now_ts=NOW, fee_pct_fn=fee_pct)["0xa"]
    assert abs(out["roi_gross"]) < 1e-9
    assert abs(out["roi_net"] - (-2.5)) < 1e-9, out["roi_net"]


def test_dollar_weighting():
    # 100% stake win at 50c (+100) vs 25% stake loss (-25): ROI = 75 / 125 = +60%
    bets = [bet("0xa", 1, "WIN", 0.5, 100), bet("0xa", 1, "LOSS", 0.5, 25)]
    out = rolling_roi.compute_rolling_roi(bets, now_ts=NOW)["0xa"]
    assert abs(out["roi_gross"] - 60.0) < 1e-9, out["roi_gross"]


def test_win_cap():
    # 10c win would be +9u; capped at +3u. With one loss: (3 - 1) / 2 = +100%
    bets = [bet("0xa", 1, "WIN", 0.10), bet("0xa", 1, "LOSS", 0.5)]
    out = rolling_roi.compute_rolling_roi(bets, now_ts=NOW, max_win_units=3.0)["0xa"]
    assert abs(out["roi_gross"] - 100.0) < 1e-9, out["roi_gross"]


def test_gross_when_no_fee_fn():
    bets = [bet("0xa", 1, "WIN", 0.4), bet("0xa", 1, "LOSS", 0.4)]
    out = rolling_roi.compute_rolling_roi(bets, now_ts=NOW)["0xa"]
    assert out["roi_net"] == out["roi_gross"]


def test_dict_input_and_junk_rows():
    bets = {"k1": bet("0xa", 1, "WIN"), "k2": "not a dict", "k3": {"wallet": "0xa"}}
    out = rolling_roi.compute_rolling_roi(bets, now_ts=NOW)
    assert out["0xa"]["n"] == 1


# ---- tiers ----------------------------------------------------------------

OLD = dict(
    suppress_enter=-10.0, suppress_exit=-5.0, cap_enter=0.0, cap_exit=3.0,
    trusted_enter=10.0, trusted_exit=7.0,
    min_n_suppress=50, min_n_cap=50, min_n_trusted=100,
)


def old_lifetime_logic(roi, resolved):
    """The inline logic this change replaced (copied from the original loop)."""
    if roi is not None and resolved >= 50 and roi <= -10.0:
        return "suppress"
    if roi is not None and resolved >= 50 and roi <= 0.0:
        return "cap_lean"
    if roi is not None and resolved >= 100 and roi >= 10.0:
        return "trusted"
    return "allow"


def test_no_hysteresis_matches_original_logic():
    rois = [None, -50, -10.01, -10, -9.99, -5, -0.01, 0, 0.01, 3, 7, 9.99, 10, 10.01, 40]
    ns = [0, 49, 50, 99, 100, 5000]
    prevs = [None, "allow", "suppress", "cap_lean", "trusted"]
    for roi, n, prev in itertools.product(rois, ns, prevs):
        got = rolling_roi.decide_tier(roi, n, prev, hysteresis=False, **OLD)
        assert got == old_lifetime_logic(roi, n), (roi, n, prev, got)


def test_hysteresis_sequences():
    kw = dict(OLD, min_n_suppress=30, min_n_cap=30, min_n_trusted=30)

    def run(rois, n=60):
        prev, seq = None, []
        for r in rois:
            prev = rolling_roi.decide_tier(r, n, prev, **kw)
            seq.append(prev)
        return seq

    # enters cap_lean at 0, stays through 0..3, leaves above 3
    assert run([2, -1, 1, 2.9, 3.1, 1]) == ["allow", "cap_lean", "cap_lean", "cap_lean", "allow", "allow"]
    # enters suppress at -10, stays until above -5, then lands in cap_lean, then allow above 3
    assert run([-12, -8, -6, -4, 1, 4]) == ["suppress", "suppress", "suppress", "cap_lean", "cap_lean", "allow"]
    # trusted enters at 10, holds down to 7, drops below 7
    assert run([11, 8, 7.5, 6.9]) == ["trusted", "trusted", "trusted", "allow"]
    # trusted does NOT enter at 8 (needs 10)
    assert run([8]) == ["allow"]


def test_trusted_needs_history_and_sample():
    kw = dict(OLD, min_n_suppress=30, min_n_cap=30, min_n_trusted=30)
    assert rolling_roi.decide_tier(15, 60, None, trusted_history_ok=False, **kw) == "allow"
    assert rolling_roi.decide_tier(15, 60, None, trusted_history_ok=True, **kw) == "trusted"
    assert rolling_roi.decide_tier(15, 29, None, trusted_history_ok=True, **kw) == "allow"


def test_small_sample_never_restricts():
    kw = dict(OLD, min_n_suppress=30, min_n_cap=30, min_n_trusted=30)
    assert rolling_roi.decide_tier(-60, 29, "suppress", **kw) == "allow"


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for t in tests:
        t()
        print(f"ok  {t.__name__}")
    print(f"{len(tests)} tests passed")
