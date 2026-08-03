"""Contract tests.

contracts/events.schema.json is what a consumer team would integrate against.
If it is only a document, it drifts from the code the first time a field is
renamed. These tests make the schema executable, so the wire format and its
published description cannot diverge silently.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator

from pulsebridge_gateway.domain.events import EventEnvelope


@pytest.fixture(scope="module")
def schema(contracts_dir: Path) -> dict[str, Any]:
    return json.loads((contracts_dir / "events.schema.json").read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def validator(schema: dict[str, Any]) -> Draft202012Validator:
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


def test_published_envelope_matches_the_schema(validator: Draft202012Validator) -> None:
    envelope = EventEnvelope.create(
        tenant_id="acme",
        event_type="order.created",
        payload={"order_id": "o-1"},
        event_id="evt-1",
        correlation_id="corr-1",
    )
    validator.validate(envelope.to_wire())


def test_generated_ids_still_satisfy_the_schema(validator: Draft202012Validator) -> None:
    # Server-generated UUIDs must fit the declared length limits, otherwise the
    # gateway emits events its own published contract rejects.
    envelope = EventEnvelope.create(tenant_id="acme", event_type="order.created", payload={})
    validator.validate(envelope.to_wire())


def test_schema_forbids_unknown_fields(schema: dict[str, Any]) -> None:
    # Consumers deserialise strictly; an unannounced field must be a contract
    # change reviewed in a pull request, not a surprise at runtime.
    assert schema["additionalProperties"] is False


def test_every_schema_field_is_produced_by_the_domain(schema: dict[str, Any]) -> None:
    envelope = EventEnvelope.create(tenant_id="acme", event_type="order.created", payload={})
    assert set(schema["properties"]) == set(envelope.to_wire())


def test_required_fields_are_never_empty(schema: dict[str, Any]) -> None:
    envelope = EventEnvelope.create(tenant_id="acme", event_type="order.created", payload={})
    wire = envelope.to_wire()
    for field in schema["required"]:
        assert wire[field] != "", f"{field} must carry a value on the wire"


def test_committed_openapi_matches_the_application(repo_root: Path, contracts_dir: Path) -> None:
    """The committed spec must match what the app actually serves.

    Reviewers and client generators read contracts/openapi.json; a stale file is
    worse than no file because it is trusted.
    """
    spec_path = contracts_dir / "openapi.json"
    assert spec_path.exists(), "run `make openapi` to generate contracts/openapi.json"

    sys.path.insert(0, str(repo_root / "scripts"))
    from export_openapi import render

    assert spec_path.read_text(encoding="utf-8") == render(), (
        "contracts/openapi.json is stale — run `make openapi`"
    )
