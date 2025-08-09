"""Tests standard tap features using the built-in SDK tests library."""

import datetime
import os

from singer_sdk.testing import get_tap_test_class

from tap_dagster.tap import TapDagster

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
    "api_token": os.getenv("TAP_DAGSTER_API_TOKEN"),
    "base_url": os.getenv("TAP_DAGSTER_BASE_URL"),
}


# Run standard built-in tap tests from the SDK:
TestTapDagster = get_tap_test_class(
    tap_class=TapDagster,
    config=SAMPLE_CONFIG,
)
