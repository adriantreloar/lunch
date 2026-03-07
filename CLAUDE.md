# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Environment Setup

Uses [uv](https://docs.astral.sh/uv/) with Python 3.10. Create/sync the environment:
```bash
bash ci/make_env.sh   # installs uv if absent, then runs: uv sync --all-extras
```

All commands below are prefixed with `uv run` so they use the managed `.venv` automatically.

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
