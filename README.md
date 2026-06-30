### Collector for FTS's Datasets
[![Build Status](https://github.com/OCHA-DAP/hdx-scraper-fts/actions/workflows/run-python-tests.yaml/badge.svg)](https://github.com/OCHA-DAP/hdx-scraper-fts/actions/workflows/run-python-tests.yaml) [![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-scraper-fts/badge.svg?branch=main&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-scraper-fts?branch=main)

This script connects to the [FTS API](https://api.hpc.tools/docs/v1/) and extracts requirements and funding data country by country creating a dataset per country in HDX. It makes in the order of 5000 reads from FTS and 1000 read/writes (API calls) to HDX in a one hour period. It saves 3 temporary files per country each less than 5 KB and these are what are uploaded to HDX. These files are then deleted. It runs every day. Plan overview, country funding trend, and flow data are fetched from the FTS API; plan IDs are mapped to countries; requirements and funding are aggregated by country, cluster, and global cluster; COVID-19 specific funding tracks are handled separately; and the per-country standard datasets (requirements/funding, cluster-level, and COVID breakdown files) are uploaded to HDX first; a global standard dataset is then generated and uploaded; and finally a HAPI funding dataset is produced from the global data.


### Usage

    python run.py

For the script to run, you will need to have a file called .hdx_configuration.yaml in your home directory containing your HDX key eg.

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: prod

 You will also need to supply the universal .useragents.yaml file in your home directory as specified in the parameter *user_agent_config_yaml* passed to facade in run.py. The collector reads the key **hdx-scraper-fts** as specified in the parameter *user_agent_lookup*.

 Alternatively, you can set up environment variables: USER_AGENT, HDX_KEY, HDX_SITE, BASIC_AUTH, EXTRA_PARAMS, TEMP_DIR, LOG_FILE_ONLY
