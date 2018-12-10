### Collector for FTS's Datasets
[![Build Status](https://travis-ci.org/OCHA-DAP/hdx-scraper-fts.svg?branch=master&ts=1)](https://travis-ci.org/OCHA-DAP/hdx-scraper-fts) [![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-scraper-fts/badge.svg?branch=master&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-scraper-fts?branch=master)

This script connects to the [FTS API](https://api.hpc.tools/docs/v1/) and extracts requirements and funding data country by country creating a dataset per country in HDX. It makes in the order of 2500 reads from FTS and 1000 read/writes (API calls) to HDX in a one hour period. It saves 3 temporary files per country each less than 5Kb and these are what are uploaded to HDX. These files are then deleted. It runs every day. 


### Usage

    python run.py

For the script to run, you will need to have a file called .hdx_configuration.yml in your home directory containing your HDX key eg.

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: prod
    
 You will also need to supply the universal .useragents.yml file in your home directory as specified in the parameter *user_agent_config_yaml* passed to facade in run.py. The collector reads the key **hdx-scraper-fts** as specified in the parameter *user_agent_lookup*.
 
 Alternatively, you can set up environment variables: USER_AGENT, HDX_KEY, HDX_SITE, BASIC_AUTH, EXTRA_PARAMS, TEMP_DIR, LOG_FILE_ONLY