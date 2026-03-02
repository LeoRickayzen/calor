from __future__ import annotations

from etl.job.constants import EXPANSION_MASKS
from etl.job.etl_helpers import primary_keys_for_segment


class TestPrimaryKeysForSegment:
    def test_keys_are_as_expected(self):
        keys = primary_keys_for_segment("borough", "hackney", "flat", "freehold")
        expected_keys = {
            "borough#hackney#flat#freehold#all#all",
            "borough#hackney#flat#all#all#all",
            "borough#hackney#all#freehold#all#all",
            "borough#hackney#all#all#all#all",
            "borough#all#flat#freehold#all#all",
            "borough#all#flat#all#all#all",
            "borough#all#all#freehold#all#all",
            "borough#all#all#all#all#all",
        }
        assert set(keys) == expected_keys
        assert len(keys) == 8
