# 0004: Flows for all years, fetched by year with country files derived

## Status

Accepted — 2026-11-10

## Context

Flow files covered only the current year, from
`custom-search?locationid=<id>&year=<current year>`. Covering all years with
per-country `custom-search?locationid=<id>&limit=1000` queries was measured on
2026-11-10:

- 251 locations have 579,761 flow rows (~731 pages at about 5.6 s each, ~70 min).
  A flow is repeated for every country it involves.
- The global incoming/internal/outgoing files concatenated the country rows. A
  USA→AFG flow appeared as outgoing and as incoming with no column saying which
  country the boundary referred to.
- Flows to no country are in no country query. In 2026, 294 of 24,700 flows
  (US$0.34bn of 29.63bn) went to Global (242), a region (19) or no location (33).

`custom-search` needs a filter: a query with none fails. `year=<Y>` returns every
flow with a usage year of Y. Over 1992–2027 that is 426,803 rows and 378,432
distinct flows (multi-year flows repeat), fetched in 53 min. `boundary` and
`onBoundary` are then relative to the year, not to a country.

## Decision

- Flows are fetched with `custom-search?year=<Y>&limit=1000` from 1990, through
  the current year, until two later years in a row have no flows (2028–2031
  had 896, 100, 22 and 1 flows). Each flow is kept once by id.
- Country flow files are derived from these flows, with `boundary` and
  `onBoundary` recalculated for the country:
  - incoming: the country is a destination location only, or both source and
    destination with `newMoney` true
  - outgoing: the country is a source location only
  - internal: the country is both source and destination with `newMoney` false
  - `onBoundary` is `shared` when the source or destination has more than one
    location, else `single`
- The rules matched `custom-search?locationid=` for all flows of AFG, JOR, HTI,
  PHL, USA (boundary) and AFG, JOR, PSE, SYR (onBoundary). The only differences
  were flows edited between the two fetches.
- The global file is one `fts_funding_global.csv` of every distinct flow,
  including flows to no country. It has no `boundary`/`onBoundary` columns. It
  replaces `fts_{incoming,internal,outgoing}_funding_global.csv`.
- `srcLocations`/`destLocations` hold ISO3 codes for countries and names for
  Global and regions (the locations FTS lists without ISO3), e.g. `AFG,Global`.
  They were blank, so flows to Global could not be told from flows with no
  location. This affects 6,538 country-file rows, mostly donors' outgoing files
  (e.g. 1,208 United States → Global flows).
- Dataset time periods start at the first requirements and funding year, not
  the earliest flow date. Flows 148776 and 147653 (2016, CAF) have date
  1900-01-01, which made the global and CAF datasets start in 1900. A
  country with flows but no requirements and funding uses its earliest flow
  date, ignoring dates before the flows start year (1990).

## Consequences

- One flow download (~53 min) replaces the per-country queries.
- All flows are held in memory until the global file is written: about 1.7 GB
  peak for 378k flows. The global CSV is about 180 MB.
- Rows with the same date are ordered by id descending.
- Resuming a run through `wheretostart` downloads all flows again.
