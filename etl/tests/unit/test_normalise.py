from __future__ import annotations

from etl.job.etl_helpers import (
    normalise,
    normalise_duration,
    normalise_postcode_prefix,
    normalise_property_type,
)


class TestNormalisePropertyType:
    def test_flat(self):
        assert normalise_property_type("F") == "flat"
        assert normalise_property_type("f") == "flat"

    def test_detached(self):
        assert normalise_property_type("D") == "detached"

    def test_empty_and_none(self):
        assert normalise_property_type("") == "other"
        assert normalise_property_type(None) == "other"

    def test_invalid_returns_other(self):
        assert normalise_property_type("X") == "other"
        assert normalise_property_type("  ") == "other"


class TestNormaliseDuration:
    def test_freehold(self):
        assert normalise_duration("F") == "freehold"
        assert normalise_duration("f") == "freehold"

    def test_leasehold(self):
        assert normalise_duration("L") == "leasehold"

    def test_empty_and_none(self):
        assert normalise_duration("") == "unknown"
        assert normalise_duration(None) == "unknown"

    def test_invalid_returns_unknown(self):
        assert normalise_duration("X") == "unknown"


class TestNormalise:
    def test_lowercase_single_space(self):
        assert normalise("  Greater  London  ") == "greater london"

    def test_empty_and_none(self):
        assert normalise("") == "unknown"
        assert normalise("   ") == "unknown"
        assert normalise(None) == "unknown"

    def test_strips_hash(self):
        assert normalise("a#b") == "ab"


class TestNormalisePostcodePrefix:
    def test_n1(self):
        assert normalise_postcode_prefix("N1 2AB") == "n1"

    def test_n15(self):
        assert normalise_postcode_prefix("N15 3EP") == "n15"

    def test_empty_and_none(self):
        assert normalise_postcode_prefix("") == "unknown"
        assert normalise_postcode_prefix(None) == "unknown"
