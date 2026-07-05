# Collector for FTS Datasets
[![Build Status](https://github.com/OCHA-DAP/hdx-scraper-fts/actions/workflows/run-python-tests.yaml/badge.svg)](https://github.com/OCHA-DAP/hdx-scraper-fts/actions/workflows/run-python-tests.yaml)
[![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-scraper-fts/badge.svg?branch=main&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-scraper-fts?branch=main)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

This script connects to the [FTS API](https://api.hpc.tools/docs/v1/) and extracts
requirements and funding data country by country creating a dataset per country in
HDX. It reads from the FTS API and makes read/write API calls
to HDX. It saves 3 temporary files per country each less than 5 KB and these are
what are uploaded to HDX, and these files are then deleted. Plan overview, country
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

- **Per-country datasets** (~one write per country): each country dataset contains
  3 files — requirements/funding, cluster-level, and COVID-19 breakdown.
- **Global standard dataset** (1 write): aggregated requirements and funding data
  across all countries.
- **HAPI funding dataset** (1 write): derived from the global data.

### Temporary files

- 3 files per country, each less than 5 KB, uploaded to HDX and then deleted.

### Uploaded files

- Per-country standard datasets: 3 CSV files per country (requirements/funding,
  cluster-level, COVID breakdown), each less than 5 KB.
- Global standard funding dataset.
- HAPI funding dataset.

### Transformations

1. **Plan-to-country mapping**: FTS plan IDs are resolved to country ISO3 codes.
2. **Funding aggregation**: requirements and funding amounts are summed by country,
   cluster, and global cluster.
3. **COVID-19 tracking**: funding flows tagged with COVID-19 are extracted and
   reported separately.

## Development

### Environment

Development is currently done using Python 3.13. Install packages with:

```shell
    pip install -r requirements.txt
```

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
    python run.py
```

### Pre-commit

Be sure to install `pre-commit`, which is run every time you make a git commit:

```shell
    pip install pre-commit
    pre-commit install
```

With pre-commit, all code is formatted according to
[ruff](https://docs.astral.sh/ruff/) guidelines.

To check if your changes pass pre-commit without committing, run:

```shell
    pre-commit run --all-files
```

## Packages

[Hatch](https://hatch.pypa.io/) is used for package management. If you've
introduced a new package to the source code (i.e. anywhere in `src/`), please
add it to the `project.dependencies` section of `pyproject.toml` with any known
version constraints.

To add packages required only for testing, add them to the `test` section under
`[project.optional-dependencies]`.

## Project

[Hatch](https://hatch.pypa.io/) is used for project management. The project can be
built using:

```shell
    hatch build
```

Linting and syntax checking can be run with:

```shell
    hatch fmt --check
```

To run the tests and view coverage, execute:

```shell
    hatch test
```
