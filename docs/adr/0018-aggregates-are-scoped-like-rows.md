# Aggregates are scoped like rows

## Status

accepted

## Context

ADR 0011 routed every admin *row* through `organisations/access.py`, and the
URL sweep that guards it drives every registered admin URL with another
organisation's ids and asserts no listing names them. That sweep looks at which
objects a response mentions. It does not look at the numbers next to them.

Numbers turned out to be a seam of their own, because a count is rarely read off
the queryset the listing already scoped. It is a second query, written next to
the first for a reason that had nothing to do with tenancy:

- The Data Feed listing computes its health chips over an explicitly *unfiltered*
  queryset, so that filtering to "Failed" still shows how many feeds are OK. That
  is right, and the comment saying so is right. But "unfiltered" was implemented
  as `DataFeed.objects.with_health()`, which is unfiltered by organisation too —
  so the chips, and the "All" total summed from them, quoted the whole instance
  while the rows underneath were correctly scoped.
- The dashboard summary tiles called `Catalog.objects.count()` directly. There is
  no listing queryset there to inherit from; a `SummaryItem` is handed a request
  and asked for a number.

Both are invisible on a single-tenant instance and stay invisible on a
multi-tenant one until somebody compares a total against the page it sits on.
Neither names another institution — which is exactly why the row-level sweep did
not catch them, and why "does it leak?" turned out to be the wrong question to
have been asking.

## Decision

**Every number rendered in the admin equals the number of rows the requesting
organisation can reach.** A count that disagrees with its own listing is a bug,
whether or not the discrepancy names anybody.

Stated as a rule rather than as three fixes because the rule is mechanically
checkable and the intuition is not. It gives an unambiguous answer where
leak-avoidance gives none: an inflated total is wrong because a national service
reading its dashboard is being told it holds catalogs it does not hold, and it
will plan against that number.

Three consequences follow directly, and are the whole of the rule:

- **A supporting query scopes even when it is not the listing's own.** The health
  chips scope through the same view mixin the listing uses. "Unfiltered" keeps
  its original meaning — the health filter and the search box — and never meant
  the organisation.
- **A count with no listing queryset behind it scopes explicitly.** The summary
  tiles reach `scoped_queryset` themselves. They also apply no `is_active`
  filter: the tiles link to the Catalog accordion, which lists active and
  inactive rows alike, and matching the linked page is the rule.
- **A number that is genuinely instance-wide says so.** `PluginSummaryItem`
  counts installed plugins, which no organisation owns and `plugin_list` shows
  all of. The rule already holds for it; narrowing it to look consistent with its
  neighbours would break the rule to satisfy an aesthetic. The exemption is named
  in a docstring, as ADR 0011 names its own.

**A roll-up built from a `NOT_ORM_SCOPABLE` model descends from an already-scoped
parent.** `FileIngestion` records a file, which may belong to nothing yet, so
`scoped_queryset` refuses it by design. The ingestion dashboard's thirty-day
history therefore narrows on the collection ids the request already scoped —
the route ADR 0011 sanctioned for rows, applied to an aggregate.

**Scoping a supporting query is by the shortest true path, not the declared
one.** The same dashboard's link table, `DataFeedCollectionLink`, does declare an
organisation path — but it runs through the feed's *nullable* catalog, so
filtering on it would drop links whose feed has no catalog and silently report
those collections as manually uploaded. It is narrowed on the scoped collection
ids instead. A declared path is permission to scope, not an instruction to scope
that way.

## Consequences

- The fixed queries are now proportional to one organisation's holdings rather
  than to the instance's. That was not the reason for the change, but it is the
  reason the two provably-safe dashboard queries were changed as well: both
  discarded the other tenants' rows, after the database had read them all, and
  the cost grew with every institution onboarded.
- Coverage is by targeted test, not by sweep. Each fixed surface has a
  two-organisation test asserting it reports only the requesting host's numbers,
  and the tiles are additionally tested from *both* hosts — a count that is
  merely smaller than the global one is not yet a count that tracks who is
  asking. This does not catch a new unscoped aggregate elsewhere. Extending the
  URL sweep to assert over rendered numbers was considered and rejected: matching
  digits in HTML is brittle enough that it would be turned off before it caught
  anything.
- The public plane needed no changes. `get_landing_stats` and the dataset index's
  per-catalog collection counts already went through `scoped_queryset` and
  `visible_visibilities`, because they were written after ADR 0012 rather than
  before it. The gap was entirely in admin surfaces that predated tenancy.
