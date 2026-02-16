from __future__ import annotations

from alekfi.brain.research_bundle import _build_forecast


def test_forecast_contract_contains_all_horizons() -> None:
    out = _build_forecast(
        sig_type="earnings_drift",
        direction="LONG",
        conviction=0.8,
        type_accuracy=0.6,
        cluster_convergence=0.5,
        independence_score=0.7,
        empirical_priors=None,
    )

    assert out["model"] == "v0_heuristic"
    for h in ("1h", "4h", "1d", "3d", "7d"):
        fh = out["by_horizon"][h]
        assert "p_up" in fh and "p_down" in fh and "p_flat" in fh
        assert "expected_move_pct" in fh and "expected_adverse_pct" in fh


def test_forecast_uses_global_priors_when_type_prior_missing() -> None:
    priors = {
        "model_name": "v1_empirical_priors",
        "global": {
            "horizons": {
                "1d": {
                    "p_correct": 0.77,
                    "exp_favorable_move_pct": 2.25,
                    "exp_adverse_move_pct": 0.95,
                }
            }
        },
        "by_signal_type": {},
    }

    out = _build_forecast(
        sig_type="unknown_new_type",
        direction="LONG",
        conviction=0.55,
        type_accuracy=None,
        cluster_convergence=0.4,
        independence_score=0.6,
        empirical_priors=priors,
    )

    h1d = out["by_horizon"]["1d"]
    assert out["model"] == "v1_empirical_priors"
    assert h1d["expected_move_pct"] == 2.25
    assert h1d["expected_adverse_pct"] == 0.95
