#!/usr/bin/env python3
"""
capacity_client.py — external client for the OptimusDB Capacity Registry.

Implements the Capacity Provider flow end to end:

    1. CP  -> OptimusDB : reserve a capID for a new capacity
    2. CP  -> OptimusDB : submit the CDT carrying that capID
    3. CP  -> RA        : capID goes into the RA configuration
    4. RA  -> OptimusDB : retrieve the CDT by capID

The capID is issued by the agent and embeds that agent's libp2p peer ID, so it
is unique across every deployed agent by construction — this client never has
to generate, negotiate or deduplicate identifiers.

Library use:

    from capacity_client import CapacityClient
    c = CapacityClient("http://193.225.250.240/optimusdb1")
    r = c.reserve("did:swarm:cp-01", "compute", region="eu-gr-athens")
    c.submit_cdt(r["cap_id"], cdt_dict)

CLI:

    python3 capacity_client.py health
    python3 capacity_client.py reserve --provider-id did:swarm:cp-01 \
                                       --capacity-type compute
    python3 capacity_client.py submit-cdt <capID> --file capacity_profile.yaml
    python3 capacity_client.py get <capID> --cdt-only
    python3 capacity_client.py list --capacity-type compute
    python3 capacity_client.py release <capID>
    python3 capacity_client.py flow --provider-id did:swarm:cp-01 \
                                    --capacity-type compute \
                                    --file capacity_profile.yaml \
                                    --ra-url http://193.225.250.240/optimusdb2

Requires: requests (PyYAML only if you pass .yaml CDTs)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from typing import Any, Dict, List, Optional

try:
    import requests
except ImportError:
    sys.exit("missing dependency: pip install requests")

try:
    import yaml
    _HAS_YAML = True
except ImportError:
    _HAS_YAML = False

__version__ = "1.0.0"

DEFAULT_URL = os.getenv("OPTIMUSDB_URL", "http://193.225.250.240/optimusdb1")
DEFAULT_TIMEOUT = 30

log = logging.getLogger("capacity")


# ──────────────────────────────────────────────────────────────────────────────
# Errors
# ──────────────────────────────────────────────────────────────────────────────

class CapacityError(RuntimeError):
    """Raised when the agent returns an error. Carries the HTTP status."""

    def __init__(self, message: str, status: Optional[int] = None,
                 payload: Any = None):
        super().__init__(message)
        self.status = status
        self.payload = payload


class CapacityNotFound(CapacityError):
    """
    The capID is not present on the agent that was queried.

    This is deliberately distinct from "does not exist". Reservations replicate
    between agents, so a 404 on one agent may simply mean replication has not
    reached it yet. Use wait_for(...) rather than treating this as fatal.
    """


# ──────────────────────────────────────────────────────────────────────────────
# Client
# ──────────────────────────────────────────────────────────────────────────────

class CapacityClient:
    """Thin, dependency-light wrapper over /api/v1/capacity."""

    def __init__(self, url: str = DEFAULT_URL, timeout: int = DEFAULT_TIMEOUT,
                 session: Optional[requests.Session] = None):
        self.url = url.rstrip("/")
        self.timeout = timeout
        self.session = session or requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": f"optimus-capacity-client/{__version__}",
        })

    # ---- plumbing ----------------------------------------------------------

    def _request(self, method: str, path: str, *, json_body: Any = None,
                 params: Optional[Dict[str, Any]] = None) -> Any:
        full = f"{self.url}{path}"
        log.debug("%s %s params=%s", method, full, params)
        try:
            resp = self.session.request(
                method, full, json=json_body, params=params,
                timeout=self.timeout,
            )
        except requests.RequestException as exc:
            raise CapacityError(f"cannot reach {full}: {exc}") from exc

        # Everything on this API returns JSON, including errors.
        try:
            payload = resp.json()
        except ValueError:
            payload = resp.text

        if resp.status_code >= 400:
            msg = payload.get("error") if isinstance(payload, dict) else str(payload)
            msg = msg or f"HTTP {resp.status_code}"
            if resp.status_code == 404:
                raise CapacityNotFound(msg, resp.status_code, payload)
            raise CapacityError(msg, resp.status_code, payload)

        log.debug("-> %s", resp.status_code)
        return payload

    # ---- step 1: reserve ---------------------------------------------------

    def reserve(self, provider_id: str, capacity_type: str, *,
                provider_name: Optional[str] = None,
                capacity_name: Optional[str] = None,
                region: Optional[str] = None,
                cdt_version: Optional[str] = None,
                expected_ra: Optional[str] = None,
                attributes: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Reserve a capID for a new capacity.

        provider_id and capacity_type are required. Returns the reservation
        record; the identifier is in ["cap_id"].
        """
        body: Dict[str, Any] = {
            "provider_id": provider_id,
            "capacity_type": capacity_type,
        }
        optional = {
            "provider_name": provider_name,
            "capacity_name": capacity_name,
            "region": region,
            "cdt_version": cdt_version,
            "expected_ra": expected_ra,
            "attributes": attributes,
        }
        body.update({k: v for k, v in optional.items() if v})
        return self._request("POST", "/api/v1/capacity/reserve", json_body=body)

    # ---- step 2: submit the CDT -------------------------------------------

    def submit_cdt(self, cap_id: str, cdt: Dict[str, Any]) -> Dict[str, Any]:
        """Attach a Capacity Description Template. Status becomes 'active'."""
        if not isinstance(cdt, dict) or not cdt:
            raise CapacityError("cdt must be a non-empty dict")
        return self._request("POST", f"/api/v1/capacity/{cap_id}/cdt",
                             json_body=cdt)

    # ---- step 4: retrieve --------------------------------------------------

    def get(self, cap_id: str) -> Dict[str, Any]:
        """Full reservation record plus an 'expired' flag."""
        return self._request("GET", f"/api/v1/capacity/{cap_id}")

    def get_cdt(self, cap_id: str) -> Dict[str, Any]:
        """Just the CDT body. Raises CapacityNotFound if none submitted yet."""
        return self._request("GET", f"/api/v1/capacity/{cap_id}",
                             params={"cdt_only": "true"})

    def wait_for(self, cap_id: str, *, timeout: int = 60,
                 interval: float = 2.0,
                 require_cdt: bool = False) -> Dict[str, Any]:
        """
        Poll until the capID is visible on this agent.

        Written for the RA side: the CP may have reserved against agent 1 while
        the RA reads from agent 2, and CRDT replication is not instantaneous.
        Raises CapacityNotFound if the deadline passes.
        """
        deadline = time.time() + timeout
        attempt = 0
        while True:
            attempt += 1
            try:
                rec = self.get(cap_id)
                if not require_cdt:
                    return rec
                reservation = rec.get("reservation", rec)
                if reservation.get("cdt_submitted"):
                    return rec
                log.debug("attempt %d: present but no CDT yet", attempt)
            except CapacityNotFound:
                log.debug("attempt %d: not replicated here yet", attempt)
            if time.time() >= deadline:
                raise CapacityNotFound(
                    f"{cap_id} not available after {timeout}s "
                    f"({attempt} attempts)" +
                    (" with a CDT" if require_cdt else ""))
            time.sleep(interval)

    # ---- list / release ----------------------------------------------------

    def list(self, **filters: str) -> Dict[str, Any]:
        """
        Filters: provider_id, capacity_type, region, status, expected_ra,
        issued_by. All exact-match and case-insensitive.
        """
        allowed = {"provider_id", "capacity_type", "region", "status",
                   "expected_ra", "issued_by"}
        bad = set(filters) - allowed
        if bad:
            raise CapacityError(f"unsupported filter(s): {', '.join(sorted(bad))}")
        params = {k: v for k, v in filters.items() if v}
        return self._request("GET", "/api/v1/capacity", params=params)

    def orphans(self) -> List[Dict[str, Any]]:
        """Reservations that were issued but never received a CDT."""
        res = self.list(status="reserved").get("reservations") or []
        return [r for r in res if not r.get("cdt_submitted")]

    def release(self, cap_id: str) -> Dict[str, Any]:
        """Withdraw a reservation. The record is kept, never reissued."""
        return self._request("DELETE", f"/api/v1/capacity/{cap_id}")

    # ---- diagnostics -------------------------------------------------------

    def agent(self) -> Dict[str, Any]:
        """Agent peer ID, role and advertised libp2p addresses."""
        return self._request("GET", "/swarmkb/agent/status")

    def stores(self) -> Dict[str, Any]:
        return self._request("GET", "/api/v1/stores")

    def health(self) -> Dict[str, Any]:
        """
        Confirm the agent answers and the capacity API is mounted.

        Note that kbcapacity only appears in the store list after the first
        reservation — the agent creates it on demand.
        """
        info = self.agent().get("agent", {})
        out = {
            "url": self.url,
            "peer_id": info.get("peer_id"),
            "role": info.get("role"),
            "addresses": info.get("addresses", []),
            "capacity_api": False,
            "kbcapacity_store": False,
        }
        try:
            self.list()
            out["capacity_api"] = True
        except CapacityError as exc:
            out["capacity_api_error"] = str(exc)
        try:
            names = [s["name"] for s in self.stores().get("stores", [])]
            out["kbcapacity_store"] = "kbcapacity" in names
        except CapacityError:
            pass
        return out


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def load_cdt(path: str) -> Dict[str, Any]:
    """Read a CDT from .json, .yaml or .yml."""
    if not os.path.isfile(path):
        raise CapacityError(f"file not found: {path}")
    with open(path, "r", encoding="utf-8") as fh:
        text = fh.read()
    if path.lower().endswith((".yaml", ".yml")):
        if not _HAS_YAML:
            raise CapacityError("PyYAML is required for YAML CDTs: pip install PyYAML")
        doc = yaml.safe_load(text)
    else:
        doc = json.loads(text)
    if not isinstance(doc, dict):
        raise CapacityError(f"{path} must contain a JSON/YAML object at the top level")
    return doc


def parse_attrs(pairs: Optional[List[str]]) -> Dict[str, Any]:
    """
    --attr num_cpus=64 --attr gpu=A100 --attr hot=true

    Values are typed: int, float, true/false, null, otherwise string.
    """
    out: Dict[str, Any] = {}
    for item in pairs or []:
        if "=" not in item:
            raise CapacityError(f"--attr expects key=value, got: {item}")
        k, v = item.split("=", 1)
        out[k.strip()] = _coerce(v.strip())
    return out


def _coerce(v: str) -> Any:
    low = v.lower()
    if low in ("true", "false"):
        return low == "true"
    if low in ("null", "none"):
        return None
    for cast in (int, float):
        try:
            return cast(v)
        except ValueError:
            pass
    return v


def emit(obj: Any) -> None:
    print(json.dumps(obj, indent=2, ensure_ascii=False))


def short(cap_id: str) -> str:
    """Trim the peer-ID segment for readable log lines."""
    parts = cap_id.split("-", 2)
    if len(parts) == 3 and len(parts[1]) > 12:
        return f"{parts[0]}-{parts[1][:6]}…{parts[1][-4:]}-{parts[2][:8]}…"
    return cap_id


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────

def cmd_health(c: CapacityClient, args) -> int:
    info = c.health()
    emit(info)
    if not info.get("capacity_api"):
        print("\ncapacity API not reachable — is the agent running the build "
              "that includes /api/v1/capacity?", file=sys.stderr)
        return 1
    return 0


def cmd_reserve(c: CapacityClient, args) -> int:
    rec = c.reserve(
        args.provider_id, args.capacity_type,
        provider_name=args.provider_name,
        capacity_name=args.capacity_name,
        region=args.region,
        cdt_version=args.cdt_version,
        expected_ra=args.expected_ra,
        attributes=parse_attrs(args.attr) or None,
    )
    if args.quiet:
        print(rec["cap_id"])
    else:
        emit(rec)
        print(f"\nissued by {rec.get('issued_by')}", file=sys.stderr)
    return 0


def cmd_submit_cdt(c: CapacityClient, args) -> int:
    emit(c.submit_cdt(args.cap_id, load_cdt(args.file)))
    return 0


def cmd_get(c: CapacityClient, args) -> int:
    if args.wait:
        c.wait_for(args.cap_id, timeout=args.wait, require_cdt=args.cdt_only)
    emit(c.get_cdt(args.cap_id) if args.cdt_only else c.get(args.cap_id))
    return 0


def cmd_list(c: CapacityClient, args) -> int:
    if args.orphans:
        items = c.orphans()
        emit({"count": len(items), "reservations": items})
        return 0
    emit(c.list(
        provider_id=args.provider_id, capacity_type=args.capacity_type,
        region=args.region, status=args.status,
        expected_ra=args.expected_ra, issued_by=args.issued_by,
    ))
    return 0


def cmd_release(c: CapacityClient, args) -> int:
    emit(c.release(args.cap_id))
    return 0


def cmd_flow(c: CapacityClient, args) -> int:
    """
    Run the whole Capacity Provider scenario, optionally reading back from a
    second agent to demonstrate that the capID resolves cluster-wide.
    """
    def step(n: int, text: str) -> None:
        print(f"\n[{n}] {text}", file=sys.stderr)

    step(1, "CP -> OptimusDB: reserve a capID")
    rec = c.reserve(
        args.provider_id, args.capacity_type,
        provider_name=args.provider_name,
        capacity_name=args.capacity_name,
        region=args.region,
        expected_ra=args.expected_ra,
        attributes=parse_attrs(args.attr) or None,
    )
    cap_id = rec["cap_id"]
    print(f"    capID     {cap_id}", file=sys.stderr)
    print(f"    issued by {rec.get('issued_by')}", file=sys.stderr)
    if rec.get("issued_by") and rec["issued_by"] in cap_id:
        print("    ✓ capID carries the issuing peer ID — unique by construction",
              file=sys.stderr)

    step(2, "CP -> OptimusDB: submit the CDT")
    updated = c.submit_cdt(cap_id, load_cdt(args.file))
    print(f"    status {updated.get('status')}  "
          f"cdt_submitted {updated.get('cdt_submitted')}", file=sys.stderr)

    step(3, "CP -> RA: capID goes into the RA configuration")
    print(f"    capID = {cap_id}", file=sys.stderr)

    step(4, "RA -> OptimusDB: retrieve the CDT by capID")
    ra = CapacityClient(args.ra_url, timeout=c.timeout) if args.ra_url else c
    if args.ra_url:
        print(f"    reading from {args.ra_url} (different agent)", file=sys.stderr)
        ra.wait_for(cap_id, timeout=args.replication_timeout, require_cdt=True)
        print("    ✓ replicated across agents", file=sys.stderr)
    cdt = ra.get_cdt(cap_id)
    print(f"    CDT retrieved, {len(cdt)} top-level key(s)", file=sys.stderr)

    print("", file=sys.stderr)
    emit({"cap_id": cap_id, "issued_by": rec.get("issued_by"),
          "status": updated.get("status"), "cdt_keys": sorted(cdt.keys())})

    if args.cleanup:
        c.release(cap_id)
        print(f"released {short(cap_id)}", file=sys.stderr)
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="capacity_client.py",
        description="Client for the OptimusDB Capacity Registry API.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--url", default=DEFAULT_URL,
                   help=f"OptimusDB agent base URL (default: {DEFAULT_URL})")
    p.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    p.add_argument("--log-level", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    p.add_argument("--version", action="version", version=__version__)

    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("health", help="agent reachability and capacity API status") \
       .set_defaults(func=cmd_health)

    def add_reserve_args(sp, need_file: bool = False):
        sp.add_argument("--provider-id", required=True, help="DID or stable provider id")
        sp.add_argument("--capacity-type", required=True,
                        help="compute | storage | network | ...")
        sp.add_argument("--provider-name")
        sp.add_argument("--capacity-name")
        sp.add_argument("--region")
        sp.add_argument("--expected-ra")
        sp.add_argument("--attr", action="append",
                        help="key=value, repeatable (e.g. --attr num_cpus=64)")
        if need_file:
            sp.add_argument("--file", required=True, help="CDT (.json/.yaml)")

    sp = sub.add_parser("reserve", help="step 1 — issue a capID")
    add_reserve_args(sp)
    sp.add_argument("--cdt-version")
    sp.add_argument("-q", "--quiet", action="store_true",
                    help="print only the capID (for shell capture)")
    sp.set_defaults(func=cmd_reserve)

    sp = sub.add_parser("submit-cdt", help="step 2 — attach the CDT")
    sp.add_argument("cap_id")
    sp.add_argument("--file", required=True, help="CDT (.json/.yaml)")
    sp.set_defaults(func=cmd_submit_cdt)

    sp = sub.add_parser("get", help="step 4 — retrieve by capID")
    sp.add_argument("cap_id")
    sp.add_argument("--cdt-only", action="store_true", help="return only the CDT body")
    sp.add_argument("--wait", type=int, metavar="SECONDS",
                    help="poll until visible on this agent (replication lag)")
    sp.set_defaults(func=cmd_get)

    sp = sub.add_parser("list", help="list / filter reservations")
    for f in ("provider-id", "capacity-type", "region", "status",
              "expected-ra", "issued-by"):
        sp.add_argument(f"--{f}")
    sp.add_argument("--orphans", action="store_true",
                    help="only reservations with no CDT")
    sp.set_defaults(func=cmd_list)

    sp = sub.add_parser("release", help="withdraw a reservation")
    sp.add_argument("cap_id")
    sp.set_defaults(func=cmd_release)

    sp = sub.add_parser("flow", help="run the full CP -> OptimusDB -> RA scenario")
    add_reserve_args(sp, need_file=True)
    sp.add_argument("--ra-url", help="second agent the RA reads from")
    sp.add_argument("--replication-timeout", type=int, default=60)
    sp.add_argument("--cleanup", action="store_true",
                    help="release the capID when done")
    sp.set_defaults(func=cmd_flow)

    return p


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(levelname)-7s %(message)s",
    )
    client = CapacityClient(args.url, timeout=args.timeout)
    try:
        return args.func(client, args)
    except CapacityNotFound as exc:
        print(f"not found: {exc}", file=sys.stderr)
        if exc.status == 404:
            print("hint: the capID may exist on another agent — retry with "
                  "--wait 60, or query the issuing agent directly", file=sys.stderr)
        return 4
    except CapacityError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
