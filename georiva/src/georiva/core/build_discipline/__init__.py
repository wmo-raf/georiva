"""The discipline a derived artefact is rebuilt under.

Some of what GeoRiva serves is not ingested but *built* from what was ingested:
a virtual-Zarr manifest over a variable's COGs, a point-forecast publication
over a collection's run. Those builds are expensive, idempotent, and triggered
by events that arrive in bursts — so every one of them needs the same
machinery, and ``virtual_zarr`` had to debug that machinery once already
(the double-dispatch race of #398).

What is shared is the *build discipline*, not the writer:

- six states — ``pending / building / ready / stale / failed / no_data``
- a claim taken in the same conditional UPDATE that decides to dispatch, so a
  record is in flight exactly once (ADR 0025)
- a claim token that travels with the task, so a copy whose claim was recycled
  stands down rather than running beside its replacement
- lock expiry as the single crash-recovery path — a claim whose task never ran
  is indistinguishable from a worker that died mid-build
- an input fingerprint, so an unchanged input skips the build
- a durable per-attempt log with retention, because the record itself only ever
  holds the *latest* state

Two grains use it. ``VirtualZarrManifest`` is per-``Variable``; a publication is
per-``Collection``. Nothing here knows which, and nothing here knows what a
build writes — see ADR 0027 for why there is no writer ABC and no registry.

``virtual_zarr`` is **not** migrated onto this base. Its shape is what the base
was extracted from and ``core/tests/test_build_discipline.py`` pins the two
together, but moving it is a separate decision with a migration attached.
"""

from .dispatch import build_attempt, dispatch_build, stand_down, sweep_builds
from .models import BuildAttemptLog, BuildDisciplinedModel

__all__ = [
    "BuildAttemptLog",
    "BuildDisciplinedModel",
    "build_attempt",
    "dispatch_build",
    "stand_down",
    "sweep_builds",
]
