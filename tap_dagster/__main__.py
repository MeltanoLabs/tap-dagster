"""Dagster entry point."""

from __future__ import annotations

from tap_dagster.tap import TapDagster

TapDagster.cli()
