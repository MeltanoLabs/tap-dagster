"""Stream type classes for tap-dagster."""

from __future__ import annotations

import math
import sys
import typing as t
from datetime import datetime, timezone
from textwrap import dedent

from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import JSONPathPaginator

from tap_dagster.client import DagsterStream

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    import requests
    from singer_sdk.helpers.types import Context, Record


class RunsPaginator(JSONPathPaginator):
    """Runs paginator."""

    @override
    def get_next(self, response: requests.Response) -> str | None:
        """Return the next page token."""
        all_matches = list(extract_jsonpath(self._jsonpath, response.json()))
        if not all_matches:
            return None

        return all_matches[-1]["runId"]


class RunsStream(DagsterStream):
    """Runs stream."""

    name = "runs"
    primary_keys: tuple[str, ...] = ("runId",)
    replication_key = "_sdc_update_time"
    records_jsonpath = "$.data.runsOrError.results[*]"

    schema = th.PropertiesList(
        th.Property(
            "runId",
            th.UUIDType,
            description="The run ID",
            required=True,
        ),
        th.Property(
            "_sdc_update_time",
            th.DateTimeType,
            description="The run update time down to the second",
            required=True,
        ),
        th.Property("status", th.StringType, description="The run status"),
        th.Property("runConfigYaml", th.StringType, description="The run config YAML"),
        th.Property("creationTime", th.NumberType, description="The run creation time"),
        th.Property("updateTime", th.NumberType, description="The run update time"),
        th.Property("startTime", th.NumberType, description="The run start time"),
        th.Property("endTime", th.NumberType, description="The run end time"),
        th.Property("parentRunId", th.UUIDType, description="The parent run ID"),
        th.Property(
            "repositoryOrigin",
            th.ObjectType(
                th.Property(
                    "repositoryLocationName",
                    th.StringType,
                    description="The repository location name",
                ),
            ),
            description="The repository origin",
        ),
    ).to_dict()

    query = dedent("""\
        query PaginatedRunsQuery($cursor: String, $updatedAfter: Float) {
            runsOrError(
                filter: {
                    updatedAfter: $updatedAfter
                }
                cursor: $cursor
                limit: 500
            ) {
                __typename
                ... on Runs {
                    count
                    results {
                        runId
                        status
                        runConfigYaml
                        creationTime
                        updateTime
                        startTime
                        endTime
                        parentRunId
                        repositoryOrigin {
                            repositoryLocationName
                        }
                    }
                }
            }
        }
    """)

    @override
    def get_new_paginator(self) -> RunsPaginator:
        """Return the new paginator."""
        return RunsPaginator(jsonpath=self.records_jsonpath)

    @override
    def post_process(self, row: Record, context: Context | None = None) -> Record | None:
        row["_sdc_update_time"] = datetime.fromtimestamp(
            float(math.floor(row["updateTime"])),  # convert decimal.Decimal to float
            tz=timezone.utc,
        )
        return row

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: str | None,
    ) -> dict[str, t.Any]:
        """Return the URL parameters."""
        params: dict[str, t.Any] = {"cursor": next_page_token}
        if bookmark := self.get_starting_timestamp(context=context):
            # We round down to the nearest second to avoid losing data.
            params["updatedAfter"] = bookmark.timestamp()

        return params
