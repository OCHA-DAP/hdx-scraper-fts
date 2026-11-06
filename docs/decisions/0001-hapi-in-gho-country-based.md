# 0001: HAPI in_gho stays country-based

## Status

Accepted — 2026-11-06

## Context

The requirements/funding files gained an `isPartOfGHO` column (HDXPIPE-202). It is
the plan's `isPartOfGHO` flag from FTS's plan overview
(`/v2/fts/flow/plan/overview/progress/<year>`), so it reflects whether that plan
was in the GHO in that year.

HAPI's `in_gho` is set from the country's current GHO status
(`Country.get_gho_status_from_iso3`). hdx-scraper-hno, hdx-scraper-iom-dtm and
hdx-scraper-wfp-foodprices do the same.

The two disagree for historical rows. Checked against the 2026-11-05 global file:

- 2020: AGO, BDI, IRQ, PHL, PRK and others had GHO plans but have `in_gho=N`.
- 2020: CUB, HND, LKA, MDG, NPL and VNM have `in_gho=Y` but had no GHO plan.
- 2023: AGO, IRQ, MWI, POL and others had GHO plans but have `in_gho=N`.

## Decision

HAPI `in_gho` stays country-based for consistency with the other HAPI pipelines.
No HAPI columns are added, removed or renamed. The discrepancy is noted in the
HAPI dataset caveats and the README.

## Consequences

- HAPI users cannot get a per-year GHO / outside-GHO split from `in_gho`. They
  need the global requirements/funding file's `isPartOfGHO`.
- Revisit if HAPI's funding schema changes, or if HAPI adopts a plan-based
  definition of `in_gho` across pipelines.
