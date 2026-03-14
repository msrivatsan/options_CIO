"""
Tests for GreeksEngine — mocked streamer/adapter, no live data needed.
"""

import time
import unittest
from unittest.mock import MagicMock, patch


def _make_streamer(live_data: dict) -> MagicMock:
    """Build a mock streamer with given live_data dict."""
    streamer = MagicMock()
    streamer.live_data = live_data

    def get_greeks(symbol):
        entry = live_data.get(symbol)
        return entry["greeks"] if entry else None

    def get_quote(symbol):
        entry = live_data.get(symbol)
        return entry["quote"] if entry else None

    def get_timestamp(symbol):
        entry = live_data.get(symbol)
        return entry.get("updated_at") if entry else None

    streamer.get_greeks = get_greeks
    streamer.get_quote = get_quote
    streamer.get_timestamp = get_timestamp
    return streamer


def _make_adapter(positions_by_portfolio: dict, accounts: dict | None = None) -> MagicMock:
    """Build a mock adapter returning given positions per portfolio."""
    adapter = MagicMock()
    adapter.get_positions = lambda pid: positions_by_portfolio.get(pid, [])
    adapter.get_accounts = lambda: accounts or {pid: MagicMock() for pid in positions_by_portfolio}
    adapter.get_market_metrics = lambda syms: {
        s: {"beta": 1.2} for s in syms
    }
    adapter.get_option_chain = lambda ticker, expiry=None: [
        {
            "symbol": f"{ticker}  260515P00050000",
            "strike_price": 50.0,
            "expiration_date": "2026-05-15",
            "option_type": "Put",
            "days_to_expiration": 45,
            "active": True,
            "is_closing_only": False,
        },
        {
            "symbol": f"{ticker}  260515C00050000",
            "strike_price": 50.0,
            "expiration_date": "2026-05-15",
            "option_type": "Call",
            "days_to_expiration": 45,
            "active": True,
            "is_closing_only": False,
        },
    ]
    return adapter


def _sample_position(symbol="IBIT  260417P00035000", qty=10, direction="Long",
                     underlying="IBIT", instrument_type="Equity Option",
                     strike=35.0, option_type="Put", expiry="2026-04-17",
                     avg_open=0.88, close_price=1.10, multiplier=100):
    return {
        "portfolio_id": "P1",
        "symbol": symbol,
        "underlying_symbol": underlying,
        "instrument_type": instrument_type,
        "quantity": qty,
        "quantity_direction": direction,
        "average_open_price": avg_open,
        "close_price": close_price,
        "multiplier": multiplier,
        "strike_price": strike,
        "option_type": option_type,
        "expiration_date": expiry,
    }


def _sample_greeks(delta=-0.35, gamma=0.02, theta=-0.05, vega=0.10, rho=0.01, iv=0.45):
    return {
        "delta": delta,
        "gamma": gamma,
        "theta": theta,
        "vega": vega,
        "rho": rho,
        "volatility": iv,
        "price": 1.05,
    }


def _sample_quote(bid=1.00, ask=1.10):
    return {
        "bid_price": bid,
        "ask_price": ask,
        "bid_size": 50.0,
        "ask_size": 50.0,
    }


class TestPositionGreeks(unittest.TestCase):
    """Test get_position_greeks multiplication and field computation."""

    def test_live_position_greeks(self):
        from options_cio.core.greeks_engine import GreeksEngine

        now = time.time()
        sym = "IBIT  260417P00035000"
        live_data = {
            sym: {
                "greeks": _sample_greeks(delta=-0.35, gamma=0.02, theta=-0.05, vega=0.10),
                "quote": _sample_quote(1.00, 1.10),
                "updated_at": now,
            }
        }
        streamer = _make_streamer(live_data)
        adapter = _make_adapter({})
        engine = GreeksEngine(adapter, streamer)

        pos = _sample_position(symbol=sym, qty=10, direction="Long", multiplier=100)
        result = engine.get_position_greeks(pos)

        self.assertEqual(result["status"], "LIVE")
        self.assertFalse(result["stale"])
        # Total = per_contract * signed_qty(10) * multiplier(100)
        self.assertAlmostEqual(result["delta"], -0.35 * 10 * 100)
        self.assertAlmostEqual(result["gamma"], 0.02 * 10 * 100)
        self.assertAlmostEqual(result["theta"], -0.05 * 10 * 100)
        self.assertAlmostEqual(result["vega"], 0.10 * 10 * 100)
        self.assertAlmostEqual(result["mark_price"], 1.05)
        self.assertEqual(result["signed_qty"], 10)

    def test_short_position_negates_qty(self):
        from options_cio.core.greeks_engine import GreeksEngine

        now = time.time()
        sym = "IBIT  260417P00035000"
        live_data = {
            sym: {
                "greeks": _sample_greeks(delta=-0.35),
                "quote": _sample_quote(),
                "updated_at": now,
            }
        }
        streamer = _make_streamer(live_data)
        engine = GreeksEngine(_make_adapter({}), streamer)

        pos = _sample_position(symbol=sym, qty=5, direction="Short")
        result = engine.get_position_greeks(pos)

        self.assertEqual(result["signed_qty"], -5)
        # Short: delta = -0.35 * (-5) * 100 = +175
        self.assertAlmostEqual(result["delta"], -0.35 * -5 * 100)

    def test_pending_when_no_greeks(self):
        from options_cio.core.greeks_engine import GreeksEngine

        sym = "MISSING  260417P00035000"
        live_data = {sym: {"greeks": None, "quote": None, "updated_at": None}}
        streamer = _make_streamer(live_data)
        engine = GreeksEngine(_make_adapter({}), streamer)

        pos = _sample_position(symbol=sym)
        result = engine.get_position_greeks(pos)

        self.assertEqual(result["status"], "PENDING")
        self.assertTrue(result["stale"])

    def test_stale_when_old_timestamp(self):
        from options_cio.core.greeks_engine import GreeksEngine

        sym = "IBIT  260417P00035000"
        live_data = {
            sym: {
                "greeks": _sample_greeks(),
                "quote": _sample_quote(),
                "updated_at": time.time() - 120,  # 2 minutes old
            }
        }
        streamer = _make_streamer(live_data)
        engine = GreeksEngine(_make_adapter({}), streamer)

        pos = _sample_position(symbol=sym)
        result = engine.get_position_greeks(pos)

        self.assertEqual(result["status"], "LIVE")
        self.assertTrue(result["stale"])


class TestPortfolioAggregation(unittest.TestCase):
    """Test get_portfolio_greeks aggregation math."""

    def _build_engine(self):
        from options_cio.core.greeks_engine import GreeksEngine

        now = time.time()
        sym1 = "IBIT  260417P00035000"
        sym2 = "IBIT  260417C00046000"

        positions = {
            "P1": [
                _sample_position(symbol=sym1, qty=10, direction="Short",
                                 underlying="IBIT", option_type="Put", strike=35.0),
                _sample_position(symbol=sym2, qty=10, direction="Long",
                                 underlying="IBIT", option_type="Call", strike=46.0),
            ]
        }
        live_data = {
            sym1: {
                "greeks": _sample_greeks(delta=-0.35, gamma=0.02, theta=-0.05, vega=0.10),
                "quote": _sample_quote(),
                "updated_at": now,
            },
            sym2: {
                "greeks": _sample_greeks(delta=0.60, gamma=0.01, theta=-0.03, vega=0.08),
                "quote": _sample_quote(),
                "updated_at": now,
            },
        }

        adapter = _make_adapter(positions)
        streamer = _make_streamer(live_data)
        return GreeksEngine(adapter, streamer)

    def test_portfolio_totals(self):
        engine = self._build_engine()
        result = engine.get_portfolio_greeks("P1")

        # sym1: Short 10 -> signed_qty=-10, delta=-0.35*-10*100=350
        # sym2: Long 10  -> signed_qty=10,  delta=0.60*10*100=600
        # Total delta = 350 + 600 = 950
        self.assertAlmostEqual(result["total_delta"], 950.0, places=2)

        # sym1 gamma: 0.02*-10*100 = -20
        # sym2 gamma: 0.01*10*100 = 10
        # Total gamma = -10
        self.assertAlmostEqual(result["total_gamma"], -10.0, places=2)

        self.assertEqual(result["position_count"], 2)
        self.assertEqual(result["pending_count"], 0)

    def test_summary_format(self):
        engine = self._build_engine()
        s = engine.summary("P1")

        self.assertIn("portfolio", s)
        self.assertIn("delta", s)
        self.assertIn("gamma", s)
        self.assertIn("theta", s)
        self.assertIn("vega", s)
        self.assertIn("position_count", s)
        self.assertEqual(s["portfolio"], "P1")

    def test_skips_non_options(self):
        from options_cio.core.greeks_engine import GreeksEngine

        positions = {
            "P1": [
                _sample_position(instrument_type="Equity"),
            ]
        }
        adapter = _make_adapter(positions)
        streamer = _make_streamer({})
        engine = GreeksEngine(adapter, streamer)

        result = engine.get_portfolio_greeks("P1")
        self.assertEqual(result["position_count"], 0)


class TestAlertFlags(unittest.TestCase):
    """Test alert thresholds fire correctly."""

    def test_gamma_risk_alert(self):
        from options_cio.core.greeks_engine import GreeksEngine

        now = time.time()
        sym = "SPX   260618P05100000"
        # gamma that when * 100 > 500
        positions = {"P2": [
            _sample_position(symbol=sym, qty=1, direction="Short",
                             underlying="SPX", instrument_type="Equity Option",
                             strike=5100, option_type="Put"),
        ]}
        live_data = {
            sym: {
                "greeks": _sample_greeks(delta=-0.40, gamma=0.06),  # total_gamma = 0.06*-1*100=-6, |6|*100=600>500
                "quote": _sample_quote(),
                "updated_at": now,
            }
        }
        engine = GreeksEngine(_make_adapter(positions), _make_streamer(live_data))
        result = engine.get_portfolio_greeks("P2")

        self.assertTrue(result["alerts"]["gamma_risk_alert"])
        self.assertGreater(result["alerts"]["gamma_risk_value"], 500)

    def test_no_gamma_risk_when_low(self):
        from options_cio.core.greeks_engine import GreeksEngine

        now = time.time()
        sym = "IBIT  260417P00035000"
        positions = {"P1": [
            _sample_position(symbol=sym, qty=1, direction="Long"),
        ]}
        live_data = {
            sym: {
                "greeks": _sample_greeks(delta=-0.35, gamma=0.001),  # |0.001*1*100|*100=10 < 500
                "quote": _sample_quote(),
                "updated_at": now,
            }
        }
        engine = GreeksEngine(_make_adapter(positions), _make_streamer(live_data))
        result = engine.get_portfolio_greeks("P1")

        self.assertFalse(result["alerts"]["gamma_risk_alert"])

    def test_vega_concentration_alert(self):
        from options_cio.core.greeks_engine import GreeksEngine

        now = time.time()
        sym = "IBIT  260417P00035000"
        # Single position = 100% vega concentration > 40%
        positions = {"P1": [
            _sample_position(symbol=sym, qty=10, direction="Long", underlying="IBIT"),
        ]}
        live_data = {
            sym: {
                "greeks": _sample_greeks(vega=0.10),
                "quote": _sample_quote(),
                "updated_at": now,
            }
        }
        engine = GreeksEngine(_make_adapter(positions), _make_streamer(live_data))
        result = engine.get_portfolio_greeks("P1")

        self.assertTrue(result["alerts"]["vega_concentration_alert"])
        self.assertEqual(result["alerts"]["vega_concentration_symbol"], "IBIT")

    def test_deep_itm_alert(self):
        from options_cio.core.greeks_engine import GreeksEngine

        now = time.time()
        sym = "IBIT  260417C00030000"
        positions = {"P1": [
            _sample_position(symbol=sym, qty=10, direction="Long",
                             option_type="Call", strike=30.0),
        ]}
        live_data = {
            sym: {
                "greeks": _sample_greeks(delta=0.85),  # deep ITM
                "quote": _sample_quote(),
                "updated_at": now,
            }
        }
        engine = GreeksEngine(_make_adapter(positions), _make_streamer(live_data))
        result = engine.get_portfolio_greeks("P1")

        self.assertTrue(result["alerts"]["deep_itm_alert"])

    def test_stale_data_alert(self):
        from options_cio.core.greeks_engine import GreeksEngine

        sym = "IBIT  260417P00035000"
        positions = {"P1": [
            _sample_position(symbol=sym, qty=10, direction="Long"),
        ]}
        live_data = {
            sym: {
                "greeks": _sample_greeks(),
                "quote": _sample_quote(),
                "updated_at": time.time() - 90,  # 90 seconds old
            }
        }
        engine = GreeksEngine(_make_adapter(positions), _make_streamer(live_data))
        result = engine.get_portfolio_greeks("P1")

        self.assertTrue(result["alerts"]["stale_data_alert"])
        self.assertEqual(result["stale_count"], 1)


class TestHedgeSuggestion(unittest.TestCase):
    """Test delta hedge recommendation."""

    def test_suggests_puts_for_positive_delta(self):
        from options_cio.core.greeks_engine import GreeksEngine

        now = time.time()
        sym = "IBIT  260417C00046000"
        positions = {"P1": [
            _sample_position(symbol=sym, qty=10, direction="Long",
                             underlying="IBIT", option_type="Call", strike=46.0),
        ]}
        live_data = {
            sym: {
                "greeks": _sample_greeks(delta=0.60),  # total = 0.60*10*100 = 600
                "quote": _sample_quote(),
                "updated_at": now,
            }
        }
        engine = GreeksEngine(_make_adapter(positions), _make_streamer(live_data))
        result = engine.suggest_delta_hedge("P1")

        self.assertIsNotNone(result)
        self.assertEqual(result["option_type"], "Put")
        self.assertEqual(result["underlying"], "IBIT")
        self.assertGreater(result["qty"], 0)
        self.assertAlmostEqual(result["current_delta"], 600.0)

    def test_no_hedge_when_delta_near_zero(self):
        from options_cio.core.greeks_engine import GreeksEngine

        now = time.time()
        sym = "IBIT  260417P00035000"
        positions = {"P1": [
            _sample_position(symbol=sym, qty=1, direction="Long"),
        ]}
        live_data = {
            sym: {
                "greeks": _sample_greeks(delta=-0.004),  # total = -0.004*1*100 = -0.4
                "quote": _sample_quote(),
                "updated_at": now,
            }
        }
        engine = GreeksEngine(_make_adapter(positions), _make_streamer(live_data))
        result = engine.suggest_delta_hedge("P1")

        self.assertIsNone(result)


class TestDataQuality(unittest.TestCase):
    """Test get_data_quality freshness reporting."""

    def test_all_fresh(self):
        from options_cio.core.greeks_engine import GreeksEngine

        now = time.time()
        sym = "IBIT  260417P00035000"
        positions = {"P1": [
            _sample_position(symbol=sym, qty=10, direction="Long"),
        ]}
        live_data = {
            sym: {
                "greeks": _sample_greeks(),
                "quote": _sample_quote(),
                "updated_at": now,
            }
        }
        engine = GreeksEngine(_make_adapter(positions), _make_streamer(live_data))
        quality = engine.get_data_quality()

        self.assertEqual(quality["system_status"], "GREEN")
        self.assertEqual(quality["stale_count"], 0)
        self.assertEqual(quality["no_data_count"], 0)

    def test_stale_goes_yellow(self):
        from options_cio.core.greeks_engine import GreeksEngine

        sym = "IBIT  260417P00035000"
        positions = {"P1": [
            _sample_position(symbol=sym, qty=10, direction="Long"),
        ]}
        live_data = {
            sym: {
                "greeks": _sample_greeks(),
                "quote": _sample_quote(),
                "updated_at": time.time() - 90,
            }
        }
        engine = GreeksEngine(_make_adapter(positions), _make_streamer(live_data))
        quality = engine.get_data_quality()

        self.assertEqual(quality["system_status"], "YELLOW")
        self.assertEqual(quality["stale_count"], 1)

    def test_missing_data_goes_yellow(self):
        from options_cio.core.greeks_engine import GreeksEngine

        sym = "IBIT  260417P00035000"
        positions = {"P1": [
            _sample_position(symbol=sym, qty=10, direction="Long"),
        ]}
        live_data = {
            sym: {
                "greeks": None,
                "quote": None,
                "updated_at": None,
            }
        }
        engine = GreeksEngine(_make_adapter(positions), _make_streamer(live_data))
        quality = engine.get_data_quality()

        self.assertEqual(quality["system_status"], "YELLOW")
        self.assertEqual(quality["no_data_count"], 1)

    def test_missing_vega_gamma_goes_yellow(self):
        from options_cio.core.greeks_engine import GreeksEngine

        sym = "IBIT  260417P00035000"
        positions = {"P1": [
            _sample_position(symbol=sym, qty=10, direction="Long"),
        ]}
        live_data = {
            sym: {
                "greeks": {"delta": 0.5, "gamma": None, "theta": -0.05,
                           "vega": None, "rho": 0.01, "volatility": 0.30, "price": 1.0},
                "quote": _sample_quote(),
                "updated_at": time.time(),
            }
        }
        engine = GreeksEngine(_make_adapter(positions), _make_streamer(live_data))
        quality = engine.get_data_quality()

        self.assertEqual(quality["system_status"], "YELLOW")


class TestSystemGreeks(unittest.TestCase):
    """Test cross-portfolio aggregation and beta adjustment."""

    def test_system_aggregation(self):
        from options_cio.core.greeks_engine import GreeksEngine

        now = time.time()
        sym1 = "IBIT  260417P00035000"
        sym2 = "SPX   260618P05100000"

        positions = {
            "P1": [_sample_position(symbol=sym1, qty=10, direction="Long", underlying="IBIT")],
            "P2": [_sample_position(symbol=sym2, qty=1, direction="Short",
                                    underlying="SPX", instrument_type="Equity Option",
                                    strike=5100, option_type="Put")],
        }
        live_data = {
            sym1: {"greeks": _sample_greeks(delta=-0.35), "quote": _sample_quote(), "updated_at": now},
            sym2: {"greeks": _sample_greeks(delta=-0.40), "quote": _sample_quote(), "updated_at": now},
        }

        engine = GreeksEngine(_make_adapter(positions), _make_streamer(live_data))
        result = engine.get_system_greeks()

        # P1: -0.35 * 10 * 100 = -350
        # P2: -0.40 * -1 * 100 = 40
        # System delta = -310
        self.assertAlmostEqual(result["system_delta"], -310.0, places=2)

        # Beta-adjusted: each position delta * beta(1.2)
        self.assertAlmostEqual(result["beta_adjusted_delta"], -310.0 * 1.2, places=2)


if __name__ == "__main__":
    unittest.main()
