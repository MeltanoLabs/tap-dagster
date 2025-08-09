"""Dagster tap class."""

from __future__ import annotations

import sys

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_dagster import streams

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


class TapDagster(Tap):
    """Dagster tap class."""

    name = "tap-dagster"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_token",
            th.StringType(nullable=False),
            required=True,
            secret=True,  # Flag config as protected.
            title="API Token",
            description="The API token to authenticate against the Dagster Cloud API",
        ),
        th.Property(
            "base_url",
            th.StringType(nullable=False),
            required=True,
            title="Base URL",
            example=[
                "http://localhost:3000",
                "https://yourorg.dagster.cloud/prod",
            ],
            description="The base URL for the Dagster Cloud API",
        ),
        th.Property(
            "start_date",
            th.DateTimeType(nullable=True),
            description="The earliest record date to sync",
        ),
    ).to_dict()

    @override
    def discover_streams(self) -> list[streams.DagsterStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.RunsStream(self),
        ]


if __name__ == "__main__":
    TapDagster.cli()
