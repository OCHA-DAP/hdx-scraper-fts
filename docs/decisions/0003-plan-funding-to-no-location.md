# 0003: Plans split by location and plan funding to no location

## Status

Accepted — 2026-11-09

## Context

22 plans in 2000–2006 and 2017 have null `countries` in the plan overview, e.g.
plan 180 (Indian Ocean Earthquake-Tsunami 2005). Their requirements and funding
were not attributed to any country.

Plans with more or less than one country take per-country amounts from
`custom-search?planid=<id>&groupby=location`. Its funding to no location (FTS's
"Not specified", id null) was left on the plan's "Multiple locations" row (see
0002), while the year's "Not specified" location row also held it.

## Decision

- **Null-country plans:** countries are filled from the locations with ISO3 in
  the plan's requirements and funding by location. The split by location is used
  even when one country results, e.g. plan 131 (2003): Zimbabwe 1.2m, no location
  309.3m.
- **Funding to no location:** for every plan split by location, it goes on a plan
  row under the "Not specified" location. That location's "Not specified" row is
  its year total − these plan rows. The plan's "Multiple locations" row no longer
  holds it.
- **Why it is not shared funding:** FTS's year total equals Σ location totals +
  `sharedFundingTotal` in every year 1999–2026 (checked live, 2026-11-09). Before,
  this funding was on two rows and the shared row was understated by it. Year
  totals are unchanged.
- **No `year=` on the plan query:** with `year=`, FTS returns no `requirements`
  block, and funding to no location was identical for all 97 plans checked.

## Consequences

- A "Not specified" location's leftover row can be negative (2000: −18.4m, 2022:
  −1.2m). The plan query counts flows that the year query marks `internal` and
  excludes, because their source received the money in the same year. Plans 56
  and 67 (2000), traced flow by flow:

  | | Plan 56 | Plan 67 | Total |
  |---|---|---|---|
  | No-location funding (plan query) | 223,644,444 | 351,505,045 | 575,149,489 |
  | of which `internal` in the year query | 31,332,652 | 9,491,031 | 40,823,683 |
  | of which no-location in the year query | 192,311,792 | 342,014,014 | 534,325,806 |

  The year's "Not specified" total is 556,736,366, so the leftover is −18,413,123.
  Without the internal flows it would be +22,410,560. Excluding them would need
  each plan's flows and the year's full flow list, so it is not done. The 2022
  gap was not traced and may have another cause.
- In 2003 (−572.6m) and 2025 (−5.1m) FTS's own "Not specified" year total is
  negative, so the leftover is too.
- Some plans have negative funding to no location (plan 189, 2005: −85.9m). It is
  kept as FTS gives it.
- Per-country funding of multi-country plans comes from the same plan query, so
  it can also include `internal` flows. This predates this record.
