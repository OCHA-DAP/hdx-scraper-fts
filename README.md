# Collector for FTS Datasets
[![Build Status](https://github.com/OCHA-DAP/hdx-scraper-fts/actions/workflows/run-python-tests.yaml/badge.svg)](https://github.com/OCHA-DAP/hdx-scraper-fts/actions/workflows/run-python-tests.yaml)
[![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-scraper-fts/badge.svg?branch=main&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-scraper-fts?branch=main)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

This script connects to the [FTS API](https://api.hpc.tools/docs/v1/) and extracts
requirements and funding data country by country creating a dataset per country in
HDX. It reads from the FTS API and makes read/write API calls
to HDX. It saves temporary CSV files per country and globally, uploads them to HDX
and then deletes them. Plan overview, country
funding trend, and flow data are fetched from the FTS API; plan IDs are mapped to
countries; requirements and funding are aggregated by country, cluster, and global
cluster; COVID-19 specific funding tracks are handled separately; the per-country
standard datasets (requirements/funding, cluster-level, and COVID breakdown files)
are uploaded to HDX first; a global standard dataset is then generated and
uploaded; and finally a HAPI funding dataset is produced from the global data.

## Data Pipeline

### API reads

- **FTS API reads**: plan overview, country funding trends, and flow
  data fetched from the [FTS API](https://api.hpc.tools/docs/v1/).
- **HDX reads**: metadata reads for existing per-country datasets
  before updating.

### API writes

- **Per-country datasets** (~one write per country): requirements/funding,
  cluster-level and COVID-19 breakdown files, plus incoming, internal and outgoing
  flow files covering all years.
- **Global standard dataset** (1 write): aggregated requirements and funding data
  across all countries.
- **HAPI funding dataset** (1 write): derived from the global data.

### Temporary files

- Per-country and global CSV files, uploaded to HDX and then deleted. The all-years
  flow files reach several MB per country and about 180 MB globally.

### Uploaded files

- Per-country standard datasets: requirements/funding, cluster-level and COVID
  breakdown files, and incoming/internal/outgoing flow files.
- Global standard funding dataset, including `fts_funding_global.csv` with every
  flow for all years.
- HAPI funding dataset.

### Transformations

1. **Plan-to-country mapping**: FTS plan IDs are resolved to country ISO3 codes.
2. **Funding aggregation**: requirements and funding amounts are summed by country,
   cluster, and global cluster.
3. **COVID-19 tracking**: funding flows tagged with COVID-19 are extracted and
   reported separately.
4. **GHO flag**: `isPartOfGHO` (Y/N) is the plan's GHO membership in that year,
   from the plan overview. "Not specified" rows (funding outside any plan) are N.
5. **Non-country rows** (global requirements/funding file only): funding to
   Global, regional and unspecified locations, funding shared across several
   locations, and plan requirements and funding not attributed to a single
   country. These rows have a blank `countryCode` and are identified by
   `location`/`locationType`. A
   warning is logged per year if the file's total funding differs from FTS's by
   more than 1%. See `docs/decisions/0002-noncountry-rows-global-file-only.md`.
6. **Flows**: every flow is downloaded year by year and kept once.
   `fts_funding_global.csv` has all of them, including flows to Global, regions
   and no location, without `boundary`/`onBoundary`. Country flow files are
   derived from the same flows, with `boundary` and `onBoundary` recalculated
   for the country. `srcLocations`/`destLocations` have ISO3 codes for countries
   and names for Global and regions. See
   `docs/decisions/0004-flows-all-years-by-year.md`.
7. **HAPI**: non-country rows are skipped. `in_gho` is the country's current GHO
   status, not the plan's `isPartOfGHO`, so the two can disagree for past years.
   See `docs/decisions/0001-hapi-in-gho-country-based.md`.

## Development

### Environment

Development is currently done using Python 3.13. The environment can be created with:

```shell
    uv sync
```

This creates a .venv folder with the versions specified in the project's uv.lock file.

### Installing and running

For the script to run, you will need to have a file called
.hdx_configuration.yaml in your home directory containing your HDX key, e.g.:

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: prod

 You will also need to supply the universal .useragents.yaml file in your home
 directory as specified in the parameter *user_agent_config_yaml* passed to
 facade in run.py. The collector reads the key **hdx-scraper-fts** as specified
 in the parameter *user_agent_lookup*.

 Alternatively, you can set up environment variables: `USER_AGENT`, `HDX_KEY`,
`HDX_SITE`, `BASIC_AUTH`, `EXTRA_PARAMS`, `TEMP_DIR`, `LOG_FILE_ONLY`.

To run, execute:

```shell
    uv run python run.py
```

### Pre-commit

pre-commit will be installed when syncing uv. It is run every time you make a git
commit if you call it like this:

```shell
    pre-commit install
```

With pre-commit, all code is formatted according to
[ruff](https://docs.astral.sh/ruff/) guidelines.

To check if your changes pass pre-commit without committing, run:

```shell
    pre-commit run --all-files
```

## Packages

[uv](https://github.com/astral-sh/uv) is used for package management.  If
you've introduced a new package to the source code (i.e. anywhere in `src/`),
please add it to the `project.dependencies` section of `pyproject.toml` with
any known version constraints.

To add packages required only for testing, add them to the
`[dependency-groups]`.

Any changes to the dependencies will be automatically reflected in
`uv.lock` with `pre-commit`, but you can re-generate the files without committing by
executing:

```shell
    uv lock --upgrade
```

## Project

[uv](https://github.com/astral-sh/uv) is used for project management. The project can be
built using:

```shell
    uv build
```

Linting and syntax checking can be run with:

```shell
    uv run ruff check
```

To run the tests and view coverage, execute:

```shell
    uv run pytest
```
