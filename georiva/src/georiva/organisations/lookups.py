"""The vocabulary of tenancy declarations, importable from anywhere.

Split from ``access.py`` so a model can declare where it stands without
importing the enforcement machinery — ``access`` reads the organisation models,
and a model importing ``access`` would close that circle.

``access`` re-exports everything here, so enforcement code has one import.
"""

#: Class attribute a model declares to name the ORM path from itself to the
#: ``Organisation`` that owns it — ``"organisation"`` on the tenancy root,
#: ``"catalog__organisation"`` one step down, and so on.
LOOKUP_ATTR = "ORGANISATION_LOOKUP"

#: Declared instead of a path by a model no organisation owns and every
#: organisation reads — topics, units, the global tier of colour palettes.
#: Scoping passes these through untouched.
SHARED_REFERENCE_DATA = "shared-reference-data"

#: Declared by a model that *is* an organisation, or is keyed by one directly.
#: Scoping matches on identity rather than following a path.
ORGANISATION_SELF = "self"

#: Declared by a model that no ORM filter on an organisation can narrow, for one
#: of two reasons. Either it belongs to an organisation but has no path to one —
#: pipeline bookkeeping keyed by a storage path (``FileIngestion`` and its jobs),
#: records reached only through an already-scoped parent (``DerivationRun``), and
#: Wagtail pages, org-owned through the Site → root-page link rather than a field
#: (decision #261) — or it belongs to a *person* rather than to an institution
#: (``ApiKey``), whose holder may be a member of several and whose credential is
#: nobody's tenant data. Scoping *refuses* these — loudly — so putting one on a
#: scoped surface is a decision somebody has to make explicitly rather than a
#: filter that quietly does nothing.
NOT_ORM_SCOPABLE = "not-orm-scopable"

#: Every declaration that is a decision rather than an ORM path.
SENTINELS = frozenset({SHARED_REFERENCE_DATA, ORGANISATION_SELF, NOT_ORM_SCOPABLE})

#: Class attribute a model sets alongside a *path* to say that a null along that
#: path is not a broken route but a tier: a row no organisation owns, which every
#: organisation reads and only the instance admin writes. Colour palettes are the
#: case the declaration exists for — a shipped library everybody draws on, plus
#: each institution's own (decision #269).
#:
#: It is deliberately not a sentinel. A model declaring it is still org-owned,
#: still filtered by the path it declared; the global rows are added to what a
#: read may see, and are refused to every write that is not the instance admin's.
GLOBAL_TIER_ATTR = "ORGANISATION_GLOBAL_TIER"

#: Models we did not write cannot declare anything — and cannot be org-owned
#: either, since nothing outside this codebase sits in the FK chain under a
#: Catalog. They are shared by construction, so the declaration requirement
#: applies to our own models only.
OWN_MODULE_PREFIX = "georiva."



def has_global_tier(model):
    """Whether ``model`` declared that a null organisation means "everybody's"."""
    return bool(getattr(model, GLOBAL_TIER_ATTR, False))


def declared_lookup(model):
    """Whatever ``model`` declared: an ORM path, one of ``SENTINELS``, or none.

    Models from outside this codebase read as shared without declaring it — see
    ``OWN_MODULE_PREFIX``.
    """
    declared = getattr(model, LOOKUP_ATTR, None)
    if declared:
        return declared
    if not model.__module__.startswith(OWN_MODULE_PREFIX):
        return SHARED_REFERENCE_DATA
    return None
