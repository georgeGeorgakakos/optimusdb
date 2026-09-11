# Changelog

All notable changes to optimusCapacity.

## [1.0.0] — 2026-09-11

First release. Client for the OptimusDB Capacity Registry, covering the
Capacity Provider → OptimusDB → Resource Agent flow.

### Added
- `CapacityClient` — `reserve`, `submit_cdt`, `get`, `get_cdt`, `wait_for`,
  `list`, `orphans`, `release`, `agent`, `stores`, `health`
- CLI: `health`, `reserve`, `submit-cdt`, `get`, `list`, `release`, `flow`
- `flow` runs all four steps; `--ra-url` reads back from a second agent to
  show the capID resolves cluster-wide
- `CapacityNotFound` as a distinct subclass of `CapacityError`, so a 404 can
  be handled as replication lag rather than a missing record
- `wait_for()` with `require_cdt` for the RA side
- CDTs accepted as `.yaml`, `.yml` or `.json`
- Typed `--attr key=value` parsing (int, float, bool, null, string)
- Packaging: `pyproject.toml`, `optimus-capacity` console script, Dockerfile,
  Makefile, venv-aware setup scripts for Linux/macOS and Windows
- 40 tests against an in-process stub agent — no OptimusDB required
- Two sample CDTs: compute (Athens GPU pool) and storage (Budapest object store)

### Notes
- Requires an OptimusDB build that includes `/api/v1/capacity`
- `/api/v1/capacity` has no load-balanced Traefik route in the K3s manifest;
  point `--url` at a per-node path such as `/optimusdb1`
