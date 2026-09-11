"""
Tests for capacity_client — run against an in-process stub agent.

No OptimusDB instance is required: a small http.server stands in for the
agent and implements just enough of /api/v1/capacity to exercise every code
path in the client, including the error and replication-lag branches.

    pip install -e ".[dev]"
    pytest

To test against a real agent instead:

    OPTIMUS_TEST_URL=http://localhost:18001 pytest -k integration
"""

import json
import os
import socketserver
import threading
import time
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs, unquote, urlparse

import pytest

from capacity_client import (
    CapacityClient,
    CapacityError,
    CapacityNotFound,
    load_cdt,
    parse_attrs,
    short,
)

# ──────────────────────────────────────────────────────────────────────────────
# Stub agent
# ──────────────────────────────────────────────────────────────────────────────

STATE = {}
PEER_ID = "QmStubPeerIDaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
# Number of GETs to fail before a capID becomes visible — simulates replication
# lag so wait_for() can be tested without a second agent.
LAG = {"remaining": 0}


class _Stub(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def _send(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # ---- GET ----
    def do_GET(self):
        path = self.path.split("?")[0]

        if path == "/swarmkb/agent/status":
            return self._send(200, {"agent": {
                "peer_id": PEER_ID,
                "role": "Coordinator",
                "addresses": ["/ip4/10.0.0.1/tcp/4001"],
            }})

        if path == "/api/v1/stores":
            stores = [{"name": "dsswres", "kind": "builtin", "address": "/orbitdb/x/dsswres"}]
            if STATE:
                stores.append({"name": "kbcapacity", "kind": "dynamic",
                               "address": "/orbitdb/x/kbcapacity"})
            return self._send(200, {"stores": stores})

        if path == "/api/v1/capacity":
            # Values are URL-encoded by requests (DIDs contain ":"), so parse
            # properly rather than string-splitting on the raw path.
            query = parse_qs(urlparse(self.path).query)
            items = list(STATE.values())
            for key in ("provider_id", "capacity_type", "region", "status",
                        "expected_ra", "issued_by"):
                if key in query:
                    want = query[key][0]
                    items = [i for i in items
                             if str(i.get(key, "")).lower() == want.lower()]
            return self._send(200, {"count": len(items), "reservations": items})

        if path.startswith("/api/v1/capacity/"):
            cap_id = unquote(path.split("/api/v1/capacity/")[1])

            if LAG["remaining"] > 0:
                LAG["remaining"] -= 1
                return self._send(404, {"error": "capID not found on this agent"})

            rec = STATE.get(cap_id)
            if rec is None:
                return self._send(404, {"error": "capID not found on this agent"})
            if "cdt_only=true" in self.path:
                if not rec["cdt_submitted"]:
                    return self._send(404, {"error": "no CDT submitted for this capID yet"})
                return self._send(200, rec["cdt"])
            return self._send(200, {"reservation": rec, "expired": False})

        self._send(404, {"error": "no route"})

    # ---- POST ----
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length) or b"{}")

        if self.path == "/api/v1/capacity/reserve":
            if not body.get("provider_id"):
                return self._send(400, {"error": "provider_id is required"})
            if not body.get("capacity_type"):
                return self._send(400, {"error": "capacity_type is required"})
            cap_id = f"cap-{PEER_ID}-{len(STATE) + 1:08d}-stub"
            STATE[cap_id] = {
                "_id": cap_id, "cap_id": cap_id, "status": "reserved",
                "issued_by": PEER_ID, "cdt_submitted": False,
                "provider_id": body["provider_id"],
                "capacity_type": body["capacity_type"],
                "region": body.get("region", ""),
                "expected_ra": body.get("expected_ra", ""),
                "attributes": body.get("attributes"),
                "store": "kbcapacity",
            }
            return self._send(201, STATE[cap_id])

        if self.path.endswith("/cdt"):
            cap_id = self.path.split("/api/v1/capacity/")[1].rsplit("/cdt", 1)[0]
            rec = STATE.get(cap_id)
            if rec is None:
                return self._send(404, {"error": f'unknown capID "{cap_id}"'})
            if rec["status"] == "released":
                return self._send(400, {"error": f'capID "{cap_id}" was released'})
            cdt = body.get("cdt") if isinstance(body.get("cdt"), dict) else body
            if not cdt:
                return self._send(400, {"error": "cdt body is empty"})
            rec.update(cdt=cdt, cdt_submitted=True, status="active")
            return self._send(200, rec)

        self._send(404, {"error": "no route"})

    # ---- DELETE ----
    def do_DELETE(self):
        cap_id = self.path.split("/api/v1/capacity/")[-1]
        rec = STATE.get(cap_id)
        if rec is None:
            return self._send(404, {"error": f'unknown capID "{cap_id}"'})
        rec["status"] = "released"
        self._send(200, rec)


@pytest.fixture(scope="module")
def stub_url():
    srv = socketserver.TCPServer(("127.0.0.1", 0), _Stub)
    srv.allow_reuse_address = True
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{srv.server_address[1]}"
    srv.shutdown()


@pytest.fixture
def client(stub_url):
    STATE.clear()
    LAG["remaining"] = 0
    return CapacityClient(stub_url, timeout=5)


# ──────────────────────────────────────────────────────────────────────────────
# Step 1 — reserve
# ──────────────────────────────────────────────────────────────────────────────

def test_reserve_returns_cap_id(client):
    rec = client.reserve("did:swarm:cp-01", "compute")
    assert rec["cap_id"].startswith("cap-")
    assert rec["status"] == "reserved"
    assert rec["cdt_submitted"] is False


def test_cap_id_embeds_issuing_peer(client):
    """The uniqueness guarantee: the identifier carries the issuer's peer ID."""
    rec = client.reserve("did:swarm:cp-01", "compute")
    assert rec["issued_by"] in rec["cap_id"]


def test_cap_id_is_printable_ascii(client):
    """Regression: a raw peer.ID cast produced binary inside the capID."""
    cap_id = client.reserve("did:swarm:cp-01", "compute")["cap_id"]
    assert cap_id.isprintable()
    assert all(0x20 <= ord(c) <= 0x7E for c in cap_id)


def test_reserve_ids_are_distinct(client):
    a = client.reserve("did:swarm:cp-01", "compute")["cap_id"]
    b = client.reserve("did:swarm:cp-01", "compute")["cap_id"]
    assert a != b


def test_optional_fields_round_trip(client):
    rec = client.reserve(
        "did:swarm:cp-01", "compute",
        region="eu-gr-athens", expected_ra="did:swarm:ra-04",
        attributes={"num_cpus": 64, "gpu": "A100"},
    )
    assert rec["region"] == "eu-gr-athens"
    assert rec["attributes"]["num_cpus"] == 64


def test_missing_provider_id_is_400(client):
    with pytest.raises(CapacityError) as exc:
        client.reserve("", "compute")
    assert exc.value.status == 400
    assert "provider_id" in str(exc.value)


def test_missing_capacity_type_is_400(client):
    with pytest.raises(CapacityError) as exc:
        client.reserve("did:swarm:cp-01", "")
    assert exc.value.status == 400


# ──────────────────────────────────────────────────────────────────────────────
# Step 2 — CDT
# ──────────────────────────────────────────────────────────────────────────────

def test_submit_cdt_activates(client):
    cap_id = client.reserve("did:swarm:cp-01", "compute")["cap_id"]
    rec = client.submit_cdt(cap_id, {"tosca_definitions_version": "tosca_simple_yaml_1_3"})
    assert rec["status"] == "active"
    assert rec["cdt_submitted"] is True


def test_submit_cdt_unknown_cap_id_is_404(client):
    with pytest.raises(CapacityNotFound):
        client.submit_cdt("cap-nope", {"a": 1})


def test_submit_empty_cdt_rejected_locally(client):
    """Caught client-side — no request is sent."""
    cap_id = client.reserve("did:swarm:cp-01", "compute")["cap_id"]
    with pytest.raises(CapacityError):
        client.submit_cdt(cap_id, {})


def test_cdt_after_release_is_400(client):
    cap_id = client.reserve("did:swarm:cp-01", "compute")["cap_id"]
    client.release(cap_id)
    with pytest.raises(CapacityError) as exc:
        client.submit_cdt(cap_id, {"a": 1})
    assert exc.value.status == 400


# ──────────────────────────────────────────────────────────────────────────────
# Step 4 — retrieval
# ──────────────────────────────────────────────────────────────────────────────

def test_get_returns_reservation(client):
    cap_id = client.reserve("did:swarm:cp-01", "compute")["cap_id"]
    assert client.get(cap_id)["reservation"]["cap_id"] == cap_id


def test_get_cdt_returns_bare_document(client):
    cap_id = client.reserve("did:swarm:cp-01", "compute")["cap_id"]
    client.submit_cdt(cap_id, {"tosca_definitions_version": "1_3", "x": 1})
    assert client.get_cdt(cap_id)["tosca_definitions_version"] == "1_3"


def test_get_cdt_before_submission_is_404(client):
    cap_id = client.reserve("did:swarm:cp-01", "compute")["cap_id"]
    with pytest.raises(CapacityNotFound):
        client.get_cdt(cap_id)


def test_unknown_cap_id_raises_not_found(client):
    with pytest.raises(CapacityNotFound):
        client.get("cap-does-not-exist")


def test_not_found_is_a_capacity_error():
    """Callers catching CapacityError must also catch the 404 case."""
    assert issubclass(CapacityNotFound, CapacityError)


# ──────────────────────────────────────────────────────────────────────────────
# Replication lag
# ──────────────────────────────────────────────────────────────────────────────

def test_wait_for_survives_replication_lag(client):
    cap_id = client.reserve("did:swarm:cp-01", "compute")["cap_id"]
    LAG["remaining"] = 2                       # two 404s, then success
    rec = client.wait_for(cap_id, timeout=10, interval=0.2)
    assert rec["reservation"]["cap_id"] == cap_id
    assert LAG["remaining"] == 0


def test_wait_for_require_cdt(client):
    cap_id = client.reserve("did:swarm:cp-01", "compute")["cap_id"]
    client.submit_cdt(cap_id, {"a": 1})
    rec = client.wait_for(cap_id, timeout=5, interval=0.2, require_cdt=True)
    assert rec["reservation"]["cdt_submitted"] is True


def test_wait_for_times_out(client):
    cap_id = client.reserve("did:swarm:cp-01", "compute")["cap_id"]
    start = time.time()
    with pytest.raises(CapacityNotFound):
        client.wait_for(cap_id, timeout=1, interval=0.2, require_cdt=True)
    assert time.time() - start >= 1


# ──────────────────────────────────────────────────────────────────────────────
# List / release / diagnostics
# ──────────────────────────────────────────────────────────────────────────────

def test_list_and_filter(client):
    client.reserve("did:swarm:cp-01", "compute", region="eu-gr-athens")
    client.reserve("did:swarm:cp-02", "storage", region="eu-hu-budapest")
    assert client.list()["count"] == 2
    assert client.list(capacity_type="compute")["count"] == 1
    assert client.list(provider_id="did:swarm:cp-02")["count"] == 1


def test_unsupported_filter_rejected(client):
    with pytest.raises(CapacityError):
        client.list(colour="blue")


def test_orphans_finds_reservations_without_cdt(client):
    a = client.reserve("did:swarm:cp-01", "compute")["cap_id"]
    client.reserve("did:swarm:cp-01", "compute")
    client.submit_cdt(a, {"a": 1})
    assert len(client.orphans()) == 1


def test_release_keeps_the_record(client):
    cap_id = client.reserve("did:swarm:cp-01", "compute")["cap_id"]
    assert client.release(cap_id)["status"] == "released"
    assert client.get(cap_id)["reservation"]["status"] == "released"


def test_health_reports_api_state(client):
    client.reserve("did:swarm:cp-01", "compute")   # creates kbcapacity
    h = client.health()
    assert h["peer_id"] == PEER_ID
    assert h["capacity_api"] is True
    assert h["kbcapacity_store"] is True


def test_unreachable_agent_raises():
    with pytest.raises(CapacityError) as exc:
        CapacityClient("http://127.0.0.1:1", timeout=2).list()
    assert "cannot reach" in str(exc.value)


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("raw,expected", [
    (["num_cpus=64"], {"num_cpus": 64}),
    (["availability=0.995"], {"availability": 0.995}),
    (["hot=true"], {"hot": True}),
    (["cold=false"], {"cold": False}),
    (["spare=null"], {"spare": None}),
    (["gpu=A100"], {"gpu": "A100"}),
    (["a=1", "b=x"], {"a": 1, "b": "x"}),
])
def test_parse_attrs_types_values(raw, expected):
    assert parse_attrs(raw) == expected


def test_parse_attrs_rejects_malformed():
    with pytest.raises(CapacityError):
        parse_attrs(["novalue"])


def test_load_cdt_json(tmp_path):
    p = tmp_path / "cdt.json"
    p.write_text('{"tosca_definitions_version": "1_3"}')
    assert load_cdt(str(p))["tosca_definitions_version"] == "1_3"


def test_load_cdt_yaml(tmp_path):
    pytest.importorskip("yaml")
    p = tmp_path / "cdt.yaml"
    p.write_text("tosca_definitions_version: tosca_simple_yaml_1_3\n")
    assert load_cdt(str(p))["tosca_definitions_version"] == "tosca_simple_yaml_1_3"


def test_load_cdt_missing_file():
    with pytest.raises(CapacityError):
        load_cdt("/nonexistent/cdt.yaml")


def test_load_cdt_rejects_non_object(tmp_path):
    p = tmp_path / "list.json"
    p.write_text('[1, 2, 3]')
    with pytest.raises(CapacityError):
        load_cdt(str(p))


def test_sample_cdt_is_valid():
    pytest.importorskip("yaml")
    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sample = os.path.join(here, "samples", "athens_gpu_pool.yaml")
    if not os.path.isfile(sample):
        pytest.skip("sample not present")
    doc = load_cdt(sample)
    assert doc["tosca_definitions_version"].startswith("tosca_")
    assert "topology_template" in doc


def test_short_trims_the_peer_segment():
    long_id = f"cap-{PEER_ID}-8f14e45f-ea2d-4c1b-9f3a-7b2e1d5c8a90"
    assert len(short(long_id)) < len(long_id)
    assert short("cap-x-y") == "cap-x-y"


# ──────────────────────────────────────────────────────────────────────────────
# Integration — only when OPTIMUS_TEST_URL points at a real agent
# ──────────────────────────────────────────────────────────────────────────────

REAL = os.getenv("OPTIMUS_TEST_URL")


@pytest.mark.integration
@pytest.mark.skipif(not REAL, reason="set OPTIMUS_TEST_URL to run")
def test_integration_full_flow():
    c = CapacityClient(REAL)
    rec = c.reserve("did:swarm:cp-pytest", "compute", region="eu-gr-athens")
    cap_id = rec["cap_id"]
    try:
        assert rec["issued_by"] in cap_id
        assert cap_id.isprintable()

        c.submit_cdt(cap_id, {"tosca_definitions_version": "tosca_simple_yaml_1_3"})
        assert c.get(cap_id)["reservation"]["status"] == "active"
        assert c.get_cdt(cap_id)["tosca_definitions_version"] == "tosca_simple_yaml_1_3"
    finally:
        c.release(cap_id)
