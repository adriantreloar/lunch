# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Environment Setup

Uses [uv](https://docs.astral.sh/uv/) with Python 3.10. Create/sync the environment:
```bash
bash ci/make_env.sh   # installs uv if absent, then runs: uv sync --all-extras
```

All commands below are prefixed with `uv run` so they use the managed `.venv` automatically.

## Documentation

### Two separate Sphinx builds

| Directory | Purpose | Command |
|-----------|---------|---------|
| `doc/design/` | **Aspirational** — describes intended behaviour that **may not yet exist in the codebase** | `uv run python -m sphinx doc/design/source doc/design/build/html` |
| `doc/reference/` | **Reference** — describes code that **exists today** in `src/lunch/` | `uv run python -m sphinx doc/reference/source doc/reference/build/html` |

The builds are independent and do not need to run together.

### Critical rules for agents

- **Design docs (`doc/design/`) do NOT reflect the current state of the code.**
  Do not assume that any class, function, module, or interface described in
  `doc/design/` exists in `src/lunch/`. Always verify against the actual code.

- **Reference docs (`doc/reference/`) MUST reflect the current state of the code.**
  If a reference doc page disagrees with the code, the doc is wrong — correct it.
  When you add or change code, update the relevant reference doc page to match.

- **Duplication is intentional.** The same concept may appear in both builds at
  different levels of completeness. That is expected and acceptable.

### Workflow

1. **Design phase** — the designer writes or refines a page in `doc/design/source/`.
2. **Approval** — the human reviews and approves the design. An agent must not
   create beads issues from a design document without human approval.
3. **Issue creation** — an agent reads the approved design document and breaks it
   into beads issues (`bd create ...`), one issue per coherent unit of work.
4. **Implementation** — issues are worked in the beads workflow: code is written,
   tests are written, and the relevant page in `doc/reference/source/` is updated
   to match the new code.
5. **Design adjustment** — if implementation reveals that the design needs to
   change, update `doc/design/source/` first, confirm with the human, raise a new
   beads issue for the adjustment, then implement.

### Which docs to read for which purpose

| Goal | Read |
|------|------|
| Understand what is currently implemented | `doc/reference/` |
| Understand what is planned / intended | `doc/design/` |
| Create beads issues for a new feature | `doc/design/` (approved pages only) |
| Refactor or debug existing code | `doc/reference/` |

### RST files only live in `doc/`

There must be no `.rst` files inside `src/`. All documentation source files
belong under `doc/design/source/` or `doc/reference/source/`.

## Beads Issue Tracking

This project uses [beads](https://github.com/steveyegge/beads) (`bd`) for issue tracking.
The beads database lives in `.beads/` (committed to git) — `bd init` has already been run and does **not** need to be repeated.

See `AGENTS.md` for the agent workflow (ready work, claiming, closing, session hand-off).

### Required external tools

Two binaries must be on `PATH` — they are **not** Python packages and are not managed by uv:

| Tool | Purpose | Windows install path (default) |
|------|---------|-------------------------------|
| `bd` | Beads CLI | `%LOCALAPPDATA%\Programs\bd` |
| `dolt` | Beads backend database | `C:\Program Files\Dolt\bin` |

`bash ci/make_env.sh` installs both automatically if they are absent.
On Windows the installers do **not** update `PATH` in the current shell session; add the paths above to your system `PATH` and restart your shell.

`beads-mcp` (the MCP server that exposes bd tools to Claude Code) is installed as a global uv tool by `ci/make_env.sh`.

### One-time Claude Code hook setup

Run once per machine (already done on the development machine):

```bash
bd setup claude   # registers SessionStart + PreCompact hooks in ~/.claude/settings.json
```

Restart Claude Code after running this for hooks to take effect.

## Commands

**Lint (format + check):**
```bash
uv run isort .
uv run black -t py310 .
uv run flake8
uv run mypy src/lunch/<module>
```

**Run all tests with coverage:**
```bash
uv run coverage run -m pytest src/lunch --junit-xml build/junit/pytest.xml
uv run coverage combine && uv run coverage html
```

**Run a single test:**
```bash
uv run pytest src/lunch/path/to/test_file.py::test_function_name
```

**Rebuild protobuf files:**
```bash
bash ci/build_protos.sh
```

## Architecture

### Goal
An MVCC (Multi-Version Concurrency Control) OLAP cube server. Data is stored in columnar parquet files, organised by version. Queries and imports are parallelised via Apache Arrow Flight and Dask.

### Version System (`src/lunch/mvcc/`)
`Version` is the core MVCC object. It carries sub-versions for each data domain:
- `model_version` — the star schema definition
- `reference_data_version` — dimension member data
- `cube_data_version` — fact/measure data
- `operations_version`, `website_version`

Every read and write operation takes a `read_version` and `write_version` pair.

### Data Model (`src/lunch/model/`)
- `StarSchema` (pyrsistent `PClass`): a `Fact` + a dict of `Dimension` objects keyed by id.
- `Fact` holds dimension references (`FactDimensionMetadatum`) and measures. `storage` on the fact describes which column ids are index vs data columns.
- Dimensions and facts are immutable pyrsistent records.

### Base Class Patterns (`src/lunch/base_classes/`)
Four strict base classes define roles:
- **`Transformer`** — stateless, only static methods. Transforms data. Easy to test.
- **`Conductor`** — stateless, holds references to other objects, delegates all work. No transformations.
- **`Stateful`** — holds mutable state (caches, stores).
- **`Data`** — pure data containers (e.g. `Plan`).

Conductors wrap free functions (the real logic lives in module-level `_private_async_functions`), keeping the class as a thin delegation layer.

### Storage Layer (`src/lunch/storage/`)
Three-layer stack per data type:
```
Store (Conductor) → Serializer (Transformer) → Persistor (Stateful)
                ↳ Cache (Stateful, optional)
```
- **Persistors**: `LocalFile*Persistor` writes to disk; `StringIO*Persistor` used in tests.
- **Serializers**: `Yaml*Serializer` for model/version/reference data; `Columnar*Serializer` for dimension data; Parquet for fact data.
- **Caches**: `Null*Cache` implementations are no-ops (used when caching is disabled).
- **Stores**: `ModelStore`, `VersionStore`, `DimensionDataStore`, `ReferenceDataStore`, `FactDataStore`.

### Manager Layer (`src/lunch/managers/`)
Managers are `Conductor` classes that expose domain operations. They combine a Store with validators/transformers:
- `ModelManager` — CRUD on star schema definitions.
- `VersionManager` — create and advance versions.
- `ReferenceDataManager` — import dimension member data.
- `CubeDataManager` — import fact/measure data.

### Import Pipeline (`src/lunch/import_engine/`)
Import follows a plan/optimise/enact pattern:
1. **Planner** (`*_planner.py`) — creates a `Plan` (pure data) describing the import steps.
2. **Optimiser** (`*_optimiser.py`) — a `Conductor` that queries storage for hints, then calls the planner. Returns a `Plan`.
3. **Enactor** (`*_enactor.py`) — executes the `Plan` by calling storage.

Plans are `Data` subclasses: `BasicPlan`, `SerialPlan`, `ParallelPlan`, `RemotePlan`.

### Flight Server (`src/lunch/flight_experiments/`)
`LunchFlightServer` (Apache Arrow Flight) is the current active server implementation. It:
- Runs a main server + client pool of sub-servers in a `ProcessPoolExecutor`.
- Handles `do_exchange` (bidirectional streaming) for dimension lookups and key sorting.
- Handles `do_put` for importing fact data from CSV and writing parquet files.
- Sub-servers are plain `LunchFlightServer` instances; the main server distributes work to them round-robin.

Command dispatch uses JSON-encoded descriptors: `{"command": "...", "parameters": {...}}`.

### Profile Service (`src/lunch/profile_service/`)
gRPC service with generated protobuf code. Used for query profiling.

### Example Scripts (`src/lunch/examples/`)
Standalone scripts showing how to wire up managers and run imports. `setup_managers.py` shows the canonical manager construction pattern. Note: hardcoded paths reference `/home/treloarja/...` — update for local use.

### Imports Convention
All imports use `src.lunch.*` as the root (e.g. `from src.lunch.mvcc.version import Version`). The package is installed in editable mode via `uv sync`, so no manual `PYTHONPATH` manipulation is needed.
