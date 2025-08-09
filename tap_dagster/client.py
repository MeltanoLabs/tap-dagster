"""GraphQL client handling, including DagsterStream base class."""

from __future__ import annotations

import sys

from singer_sdk.streams import GraphQLStream

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


class DagsterStream(GraphQLStream):
    """Dagster stream class."""

    path = "/graphql"

    @property
    @override
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["base_url"]

    @property
    @override
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        return {
            "Dagster-Cloud-Api-Token": self.config.get("api_token"),
        }
