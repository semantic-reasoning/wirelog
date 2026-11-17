#!/usr/bin/env python3
"""Timing schema checks must tolerate independently quantized measurements."""
import unittest

from validate_bench_flowlog_tdd_json import validate_exchange_phases


KEYS = ("matrix", "coordinator", "scatter", "gather", "broadcast")


def sample(exchange=0.0):
    return {
        "tdd_phase_ms": {"exchange": exchange},
        "tdd_exchange_phase_ms": dict.fromkeys(KEYS, 0.0),
    }


class ExchangeTimingTests(unittest.TestCase):
    def test_positive_parent_with_zero_children(self):
        # Synthetic tick sizes, not a measured Windows runner resolution.
        for parent in (1.0, 15.625):
            with self.subTest(parent=parent):
                validate_exchange_phases(sample(parent))

    def test_all_zero(self):
        validate_exchange_phases(sample())

    def test_nonzero_breakdown(self):
        row = sample(1.0)
        row["tdd_exchange_phase_ms"].update(matrix=0.25, broadcast=0.5)
        validate_exchange_phases(row)

    def test_missing_or_invalid_objects(self):
        for key in ("tdd_phase_ms", "tdd_exchange_phase_ms"):
            for value in (None, [], "invalid"):
                with self.subTest(key=key, value=value):
                    row = sample()
                    row[key] = value
                    with self.assertRaises(AssertionError):
                        validate_exchange_phases(row)
            row = sample()
            del row[key]
            with self.subTest(missing=key), self.assertRaises(AssertionError):
                validate_exchange_phases(row)

    def test_missing_or_invalid_parent(self):
        for value in (None, "invalid", -1.0):
            with self.subTest(value=value):
                row = sample(value)
                with self.assertRaises(AssertionError):
                    validate_exchange_phases(row)
        row = sample()
        del row["tdd_phase_ms"]["exchange"]
        with self.assertRaises(AssertionError):
            validate_exchange_phases(row)

    def test_missing_or_invalid_child(self):
        for key in KEYS:
            for value in (None, "invalid", -0.1):
                with self.subTest(key=key, value=value):
                    row = sample()
                    row["tdd_exchange_phase_ms"][key] = value
                    with self.assertRaises(AssertionError):
                        validate_exchange_phases(row)
            row = sample()
            del row["tdd_exchange_phase_ms"][key]
            with self.subTest(missing=key), self.assertRaises(AssertionError):
                validate_exchange_phases(row)

    def test_subtotal_tolerance(self):
        # The existing absolute tolerance is 1ms when the parent is 1ms.
        row = sample(1.0)
        row["tdd_exchange_phase_ms"]["matrix"] = 2.0
        validate_exchange_phases(row)
        row["tdd_exchange_phase_ms"]["matrix"] = 2.01
        with self.assertRaises(AssertionError):
            validate_exchange_phases(row)

    def test_subtotal_relative_tolerance(self):
        row = sample(8.0)
        row["tdd_exchange_phase_ms"]["matrix"] = 10.0
        validate_exchange_phases(row)
        row["tdd_exchange_phase_ms"]["matrix"] = 10.01
        with self.assertRaises(AssertionError):
            validate_exchange_phases(row)


if __name__ == "__main__":
    unittest.main()
