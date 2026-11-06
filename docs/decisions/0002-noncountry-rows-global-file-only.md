# 0002: Non-country rows in the global requirements/funding file only

## Status

Accepted — 2026-11-06

## Context

The global requirements/funding file did not reconcile with FTS totals
(HDXPIPE-202). For 2025 its funding summed to 23.67bn against FTS's 28.27bn. The
missing funding was:

- funding to "Global" (3.84bn) and "Region - …" locations (~0.23bn)
- FTS's "Not specified" location
- funding shared across several locations (`sharedFundingTotal`, 0.28bn)
- countries with funding but no plans

Requirements of multi-country plans that FTS does not split by country were also
missing.

Three ways to label rows that have no ISO3 code were considered:

- (a) blank `countryCode`, location in `name`
- (b1) new `location`/`locationType` columns in the global file only
- (b2) the same columns in every requirements/funding file

## Decision

- **Labelling:** option b1. `fts_requirements_funding_global.csv` gains `location`
  (FTS location name) and `locationType` (country, global, region, other, unknown,
  multiple). `countryCode` is blank on non-country rows.
- **Non-country locations:** each gets rows built like country rows: plan rows
  from `custom-search?locationid=<id>&year=<year>&groupby=plan`, plus "Not
  specified" = location single-location total − plan rows.
- **Unattributed plan amounts:** each plan with more or less than one country
  (or whose only location has no ISO3, e.g. plan 996 in 2020) gets a "Multiple
  locations" row. It holds plan requirements − Σ the plan's rows, and plan funding
  − Σ the plan's rows. The funding is shared across locations (2020: 273m, e.g.
  3RP 129m).
- **Shared funding:** one "Multiple locations" row per year with
  `sharedFundingTotal` minus the funding on the plan rows above.
- **Countries without plans:** these get "Not specified" rows. A country with no
  dataset appears in the global file only.
- **HAPI:** skips rows whose `locationType` is not country.

## Consequences

- Country files keep their header apart from `isPartOfGHO`.
- GHO requirements and funding equal FTS's plan overview totals. This was
  checked live for 2020, 2024 and 2025.
- Summing all global rows gives a total within ~1% of FTS's (2025 +0.01%,
  2024 +0.06%, 2020 +0.40%). A warning is logged per year when it does not.
- The remaining gap is negative amounts that FTS's year-level location breakdown
  gives some donor countries as destinations (2020: Netherlands −33.3m, Germany
  −25.1m). FTS's global total includes them. FTS's country pages and the country
  trends endpoint, which country rows use, exclude them: Netherlands 2020 shows
  2.5m on both.
- The FTS Power BI report (fts.unocha.org/home/2026/powerbi, data as of
  2026-11-05) was compared against a 2026-11-06 dry run:
  - **2025 and 2024 totals match:** 28,274.3m vs 28,274.8m, and 37,119.5m vs
    37,119.3m.
  - **2020 GHO funding matches:** 19,502.5m vs 19,502.3m.
  - **Country figures match:** the report uses the same per-country figures as
    FTS country pages (Netherlands 2.5m, Germany 2.9m), and the same Global
    figure (2,243.2m).
  - **2020 total differs:** 29,046.0m vs 29,157.8m. The report's "*Not
    Specified" (1,004.7m) plus "*(Multi-Location)" (203.6m) is 119.7m less than
    the file's "Not specified" location plus "Multiple locations" rows. Its
    split between those two buckets cannot be rebuilt from the API.
  - **API total does not fit:** adjusting to the API year total would fix 2020
    but break 2024, where the report (37,119.5m) does not equal the API total
    (37,097m). The adjustment was therefore not made.
  - **2020 GHO required is 56.5m higher in the report:** 38,593.2m vs the plan
    overview's 38,536.7m. The cause is unknown.
  - **Exact match:** would need the FTS team to explain how the report builds
    these buckets.
- Per-location plan totals include funding shared with other locations, so a
  non-country "Not specified" row can be smaller than expected, or negative.
- Users who summed the whole global file before will now get larger totals.
