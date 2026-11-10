# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**hdx-scraper-fts** connects to the [FTS API](https://api.hpc.tools/docs/v1/) and extracts
requirements and funding data country by country, creating a dataset per country in HDX. It
reads plan overview, country funding trend, and flow data from the FTS API; maps plan IDs to
countries; aggregates requirements and funding by country, cluster, and global cluster; handles
COVID-19 specific funding tracks separately; uploads per-country standard datasets
(requirements/funding, cluster-level and COVID breakdown files, plus all-years incoming,
internal and outgoing flow files) to HDX; then generates and uploads a global standard
dataset; and finally produces and uploads a HAPI funding dataset derived from the global data.

## Commands

Install dependencies:
```bash
uv sync
```

Run the scraper:
```bash
uv run python run.py
```

Run tests:
```bash
uv run pytest
```

Run a single test:
```bash
uv run pytest tests/test_fts.py
```

Lint check:
```bash
pre-commit run --all-files
```

## Architecture

The pipeline in `__main__.py`:

1. **`main`** — Reads HDX configuration, checks write access, then within a temp batch folder:
   - `FTSDownload` (`download.py`) wraps FTS API calls.
   - `Locations` (`locations.py`) builds country name/id/iso3 lookups from the FTS locations
     endpoint, restricted to countries HDX recognises.
   - `Pipeline` (`pipeline.py`) walks plan overview data year by year via `get_plans`, builds
     plan-to-country and plan-to-code mappings, and coordinates:
     - `RequirementsFunding` (`requirements_funding.py`) — per-country annual requirements/funding.
     - `RequirementsFundingCluster` (`requirements_funding_cluster.py`) — cluster-level and
       global-cluster-level requirements/funding.
     - `RequirementsFundingCovid` (`requirements_funding_covid.py`) — COVID-19 specific funding
       breakdown, derived from plans spanning 2020 onwards.
     - `Flows` (`flows.py`) — downloads every flow year by year (`get_flows`), writes one
       global file of distinct flows and derives each country's incoming/internal/outgoing
       files from them.
     - `NonCountryFunding` (`noncountry_funding.py`) — global-file-only rows for funding to
       non-country locations (Global, regions, unspecified), shared funding and unattributed
       plan requirements and funding, plus a per-year reconciliation warning against FTS's total.
   - `DatasetGenerator` (`dataset_generator.py`) builds per-country and global `Dataset`/`Showcase`
     objects and uploads them to HDX.
   - `HAPIOutput` (`hapi_output.py`) derives a HAPI funding dataset from the global results and
     uploads it.
   - All of `Flows`, `RequirementsFunding`, `RequirementsFundingCluster`, and
     `RequirementsFundingCovid` extend `ResourceGenerator` (`resource_generator.py`), which
     writes rows to a CSV file and attaches it as a dataset resource.

### Key design points

- **Temporary files**: country and global CSVs are written to a temp folder, uploaded to HDX
  as dataset resources, then deleted. All-years flow files reach MBs per country and ~180 MB
  globally.
- **Flows** are fetched by year, not by country, and held in memory (~1.7 GB peak). Country
  `boundary`/`onBoundary` are recalculated from source/destination locations
  (see `docs/decisions/0004-flows-all-years-by-year.md`).
- **Config-driven metadata**: static dataset metadata (title, tags, license, etc.) lives in
  `config/hdx_dataset_static.yaml` and `config/hdx_hapi_dataset_static.yaml`; runtime settings
  (API base URL, notes template) live in `config/project_configuration.yaml`.
- **Batch tracking**: `wheretostart_tempdir_batch` / `progress_storing_folder` allow the run to
  resume partway through the country list if interrupted.
- **Global file layout**: `fts_requirements_funding_global.csv` has extra `location`/`locationType`
  columns (header list `requirements_global_headers` in `project_configuration.yaml`); country
  files do not. Countries with no dataset (no flows) still get rows in the global file.
- **HAPI `in_gho`** is country-based, unlike the plan-based `isPartOfGHO`
  (see `docs/decisions/0001-hapi-in-gho-country-based.md`).
- **Tests** read fixtures from local `tests/fixtures/input/` (`conftest.py` sets `base_url`).

## Decision Records

Non-trivial design decisions are recorded in `docs/decisions/` (see `docs/decisions/README.md`) —
the distilled decision, not the full planning narrative, belongs here.

## Environment

Requires `~/.hdx_configuration.yaml` with HDX credentials, or env vars: `HDX_KEY`, `HDX_SITE`,
`USER_AGENT`, `BASIC_AUTH`, `EXTRA_PARAMS`, `TEMP_DIR`, `LOG_FILE_ONLY`.

Requires `~/.useragents.yaml` with a `hdx-scraper-fts` entry.

## Collaboration Style

- Be objective, not agreeable. Act as a partner, not a sycophant. Push back when you disagree, flag tradeoffs honestly, and don't sugarcoat problems.
- Keep explanations brief and to the point.
- Don't rely on recalled knowledge for facts that could be stale (API behaviour, library versions, external systems). Search or read the actual source first.

## Scope of Changes

When fixing a bug or addressing PR feedback, change only what is necessary to resolve the specific issue. Do not refactor surrounding code, rename variables, adjust formatting, or make improvements in the same commit unless they are directly required by the fix.
