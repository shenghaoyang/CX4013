"""
test_numeric.py

Tests for numeric type serialization.
"""

import pytest
from serialization import numeric


class TestNumerics:
    TYPES = (numeric.i8, numeric.i32, numeric.i64, numeric.u8, numeric.u32, numeric.u64)

    def test_cannot_overflow_or_underflow(self):
        for t in self.TYPES:
            v = t()
            over = v.max() + 1
            under = v.min() - 1

            with pytest.raises(OverflowError):
                v.value = over
            with pytest.raises(OverflowError):
                v.value = under

    def test_round_trip(self):
        for t in self.TYPES:
            v = t()
            for value in (v.max() // 2, v.min() // 2):
                v.value = value
                recovered = t.deserialize(v.serialize())
                assert recovered.value == value

    def test_correct_serialized_size(self):
        for t in self.TYPES:
            v = t()
            assert len(v.serialize()) == v.size
